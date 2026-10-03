"""Local OCR through macOS Vision (offline; no page content leaves the machine)."""


def ocr_png(png: bytes, languages=("en-US",)) -> str:
    import Foundation
    import Vision

    handler = Vision.VNImageRequestHandler.alloc().initWithData_options_(
        Foundation.NSData.dataWithBytes_length_(png, len(png)), None)
    req = Vision.VNRecognizeTextRequest.alloc().init()
    req.setRecognitionLevel_(Vision.VNRequestTextRecognitionLevelAccurate)
    req.setRecognitionLanguages_(list(languages))
    ok, err = handler.performRequests_error_([req], None)
    if not ok:
        raise RuntimeError(f"Vision OCR failed: {err}")
    return "\n".join(o.topCandidates_(1)[0].string() for o in (req.results() or []))


def ocr_page(page, dpi: int = 150, languages=("en-US",)) -> str:
    return ocr_png(page.get_pixmap(dpi=dpi).tobytes("png"), languages)
