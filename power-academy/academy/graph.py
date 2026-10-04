def validate_graph(concepts: list) -> list:
    by_id = {c["id"]: c for c in concepts}
    errs = []
    for c in concepts:
        for p in c["prerequisites"]:
            if p not in by_id:
                errs.append(f"{c['id']}: unknown prerequisite '{p}'")
        if c["originality"] == "synthesized" and not c["sources"]:
            errs.append(f"{c['id']}: synthesized concept has no sources")

    state = {}  # id -> 1 visiting, 2 done

    def visit(cid, path):
        if state.get(cid) == 2:
            return
        if state.get(cid) == 1:
            errs.append("cycle: " + " -> ".join(path + [cid]))
            return
        state[cid] = 1
        for p in by_id[cid]["prerequisites"]:
            if p in by_id:
                visit(p, path + [cid])
        state[cid] = 2

    for cid in by_id:
        visit(cid, [])

    for c in concepts:
        cur, hops = c, 0
        while cur["prerequisites"] and hops <= len(by_id):
            nxt = next((by_id[p] for p in cur["prerequisites"] if p in by_id), None)
            if nxt is None:
                break
            cur, hops = nxt, hops + 1
        if not cur["prerequisites"] and cur["level"] != "foundation":
            errs.append(f"{c['id']}: chain ends at non-foundation root '{cur['id']}'")
    return errs
