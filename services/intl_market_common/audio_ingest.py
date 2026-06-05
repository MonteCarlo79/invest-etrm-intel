"""Audio ingestion for domain expert voice memos.

Pipeline:
  1. Transcribe  : OpenAI Whisper API  (much better than iPhone built-in)
  2. Contextualize: Claude Haiku       (fixes domain terms, structures content, extracts key points)
  3. Store        : intl_market.{prefix}knowledge_docs

Supports: .m4a, .mp3, .mp4, .wav, .webm, .mpeg, .mpga
Files > 24 MB are split into 10-minute chunks via pydub (requires ffmpeg in container).

Usage:
    from services.intl_market_common.audio_ingest import transcribe_and_contextualize, store_voice_memo
"""
from __future__ import annotations

import io
import logging
from datetime import date

logger = logging.getLogger(__name__)

# OpenAI Whisper limit per request
_WHISPER_MAX_BYTES = 24 * 1024 * 1024  # 24 MB


# ── Transcription ─────────────────────────────────────────────────────────────

def _transcribe_file(audio_buf: io.BytesIO, client) -> str:
    """Send a single audio buffer to Whisper and return the plain-text transcript."""
    resp = client.audio.transcriptions.create(
        model="whisper-1",
        file=audio_buf,
        response_format="text",
    )
    return str(resp).strip()


def _transcribe_chunked(audio_bytes: bytes, filename: str, client) -> str:
    """Split audio file into 10-minute chunks and transcribe each separately."""
    try:
        from pydub import AudioSegment
    except ImportError:
        raise RuntimeError("pydub not installed — cannot split large audio files")

    ext = filename.rsplit(".", 1)[-1].lower()
    audio = AudioSegment.from_file(io.BytesIO(audio_bytes), format=ext)
    logger.info("[audio_ingest] Duration: %.1f min — splitting into chunks", len(audio) / 60000)

    chunk_ms = 10 * 60 * 1000  # 10 minutes per chunk
    n_chunks = (len(audio) + chunk_ms - 1) // chunk_ms
    parts = []

    for i in range(n_chunks):
        chunk = audio[i * chunk_ms: (i + 1) * chunk_ms]
        buf = io.BytesIO()
        chunk.export(buf, format="mp3")
        buf.seek(0)
        buf.name = f"chunk_{i:02d}.mp3"
        logger.debug("[audio_ingest] Transcribing chunk %d/%d (%d bytes)", i + 1, n_chunks, len(buf.getvalue()))
        try:
            text = _transcribe_file(buf, client)
            parts.append(text)
        except Exception as exc:
            logger.warning("[audio_ingest] Chunk %d failed: %s", i, exc)
            parts.append(f"[Chunk {i+1} transcription failed: {exc}]")

    return "\n\n".join(parts)


def transcribe_audio(audio_bytes: bytes, filename: str, openai_api_key: str) -> str:
    """Transcribe audio using OpenAI Whisper API.

    Handles files larger than 24 MB by splitting into 10-minute chunks.
    Returns raw transcript text.
    """
    import openai
    client = openai.OpenAI(api_key=openai_api_key)

    if len(audio_bytes) > _WHISPER_MAX_BYTES:
        return _transcribe_chunked(audio_bytes, filename, client)

    ext = filename.rsplit(".", 1)[-1].lower()
    buf = io.BytesIO(audio_bytes)
    buf.name = filename
    return _transcribe_file(buf, client)


# ── Contextualisation ─────────────────────────────────────────────────────────

_DOMAIN_TERMS = {
    "Philippines": (
        "WESM, IEMOP, ERC, DOE, NGCP, GEAP, PSA, RCOA, FIT, VRES, "
        "BESS, IPP, GEF, RE Act, EPIRA, SIPP, DU, AGRA, ancillary services, "
        "peaking plants, baseload, grid code, ASEAN interconnection"
    ),
    "Poland": (
        "PSE, URE, RES, CfD, auction, capacity market, Rynek Mocy, "
        "FCR, aFRR, mFRR, balancing market, OZE, TGE, grid code, "
        "DSO, TSO, BESS, prosument, RES obligation"
    ),
}


def contextualize_transcript(
    raw_transcript: str,
    anthropic_api_key: str,
    market_name: str,
    speaker_context: str = "",
) -> tuple[str, str]:
    """Post-process raw Whisper transcript with Claude for domain awareness.

    Returns (polished_transcript_with_key_points, suggested_title).
    """
    import anthropic
    client = anthropic.Anthropic(api_key=anthropic_api_key)

    # Pick the right domain term hint
    domain_hint = ""
    for key, terms in _DOMAIN_TERMS.items():
        if key.lower() in market_name.lower():
            domain_hint = f"\nKey domain terms to watch for: {terms}"
            break

    ctx_line = f"\nSpeaker/meeting context: {speaker_context}" if speaker_context else ""

    prompt = (
        f"You are processing a raw speech-to-text transcription of a domain expert interview "
        f"about {market_name} energy markets.\n\n"
        f"The raw transcript often contains errors because speech-to-text software does not know "
        f"industry terminology. Your job is to fix it while preserving every factual claim exactly.\n"
        f"{domain_hint}"
        f"{ctx_line}\n\n"
        "What to do:\n"
        "1. Correct domain term spellings that were misheard (e.g. 'west m' → WESM, 'I-MOP' → IEMOP, "
        "'cheap' → GEAP, 'end GCP' → NGCP, 'mFRR' / 'em FRR' → mFRR, etc.)\n"
        "2. Remove filler words (um, uh, you know, like) and clean up false starts\n"
        "3. Add proper punctuation and paragraph breaks\n"
        "4. If multiple speakers are detectable, label them [Speaker A:] / [Speaker B:]\n"
        "5. Do NOT add, invent, or change any factual content — only clean up presentation\n\n"
        "After the cleaned transcript, add a section:\n\n"
        "## Key Points\n"
        "(3–8 bullet points of the most important facts, figures, or claims in the recording)\n\n"
        "## Action Items\n"
        "(any follow-up tasks or next steps mentioned, or 'None identified')\n\n"
        f"Raw transcript:\n---\n{raw_transcript[:8000]}\n---\n\n"
        "Return the cleaned transcript followed by Key Points and Action Items."
    )

    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    polished = resp.content[0].text.strip()

    # Generate a concise title
    title_resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=60,
        messages=[{
            "role": "user",
            "content": (
                f"Give a concise title (max 10 words) for this {market_name} expert interview. "
                f"Just the title, no quotes.\n\n{polished[:600]}"
            ),
        }],
    )
    title = title_resp.content[0].text.strip().strip('"').strip("'")

    return polished, title


# ── Full pipeline ─────────────────────────────────────────────────────────────

def transcribe_and_contextualize(
    audio_bytes: bytes,
    filename: str,
    openai_api_key: str,
    anthropic_api_key: str,
    market_name: str,
    speaker_context: str = "",
) -> dict:
    """Full pipeline: transcribe → contextualize → return document dict.

    Returns:
        {
          "raw_transcript": str,
          "polished_content": str,
          "title": str,
          "file_size_mb": float,
        }
    Raises on transcription failure.
    """
    size_mb = len(audio_bytes) / (1024 * 1024)
    logger.info("[audio_ingest] Processing %s (%.1f MB)", filename, size_mb)

    raw = transcribe_audio(audio_bytes, filename, openai_api_key)
    if not raw.strip():
        raise RuntimeError("Whisper returned an empty transcript — check the audio file")

    polished, title = contextualize_transcript(raw, anthropic_api_key, market_name, speaker_context)

    return {
        "raw_transcript": raw,
        "polished_content": polished,
        "title": title,
        "file_size_mb": round(size_mb, 2),
    }


def store_voice_memo(
    result: dict,
    filename: str,
    conn,
    prefix: str,
    custom_title: str | None = None,
) -> None:
    """Insert a processed voice memo into the knowledge_docs table."""
    title = (custom_title or result["title"])[:250]
    # url key is filename-based so re-uploads replace previous version
    url_key = f"voice_memo://{filename}"
    content = (
        f"[Voice Memo: {filename}  |  {result['file_size_mb']} MB]\n\n"
        f"{result['polished_content']}\n\n"
        "---\nRaw transcript:\n"
        f"{result['raw_transcript'][:3000]}"
    )

    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO intl_market.{prefix}knowledge_docs "
            "(source, doc_type, title, url, published_date, content) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (url) DO UPDATE SET "
            "  content=EXCLUDED.content, title=EXCLUDED.title, fetched_at=NOW()",
            ("voice_memo", "interview", title, url_key, date.today(), content),
        )
    conn.commit()
