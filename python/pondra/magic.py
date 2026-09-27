"""`%load_ext pondra`: SQL cells in a notebook, on the newest connection (`pondra.local` or
`pondra.connect`), naming the notebook's Python frames and data as tables.

    %%sql                       ->  shows the answer (a frame)
    %%sql top_users <<          ->  the frame goes into `top_users` instead
    %sql SELECT count(*) FROM events
"""
import re


def register(ipython):
    from IPython.core.magic import register_line_cell_magic

    @register_line_cell_magic
    def sql(line, cell=None):
        from . import client
        if client._last is None:
            raise RuntimeError("no connection yet: pondra.local(…) or pondra.connect(…) first")
        target = None
        if cell is not None:
            m = re.fullmatch(r"\s*(\w+)\s*<<\s*", line)
            target, text = (m.group(1), cell) if m else (None, (line + "\n" + cell).strip())
        else:
            text = line
        frame = _in(ipython.user_ns, client._last, text)
        if target:
            ipython.user_ns[target] = frame
            return None
        return frame

    del sql


def _in(namespace, con, text):
    """`con.sql(text)` as if called from the notebook's own namespace (its names are the cell's)."""
    return eval("__pondra_con.sql(__pondra_text)", namespace, {"__pondra_con": con, "__pondra_text": text})

