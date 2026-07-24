#!/usr/bin/env python3
"""
Build a single self-contained, ReadTheDocs-style HTML from the engine's markdown docs.

No server, no Sphinx toolchain: produces one `engine-docs.html` you open in a browser. Regenerate
after editing any source doc with:  python build_docs.py

Requires: markdown, pygments  (pip install markdown pygments --break-system-packages)
"""
import os
import re
import sys
import markdown
from pygments.formatters import HtmlFormatter

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)  # model/

# (sidebar key, display title, source path). Order = sidebar order.
SOURCES = [
    ("arch",   "Engine architecture & usage", os.path.join(ROOT, "engine-java", "ARCHITECTURE.md")),
    ("parity", "Parity tests & checks",       os.path.join(ROOT, "PARITY_TESTS.md")),
]

OUT = os.path.join(HERE, "engine-docs.html")


def convert(md_text, key):
    """Render one markdown doc; return (html, toc_tokens) with ids prefixed by `key` to keep
    anchors unique across docs."""
    md = markdown.Markdown(extensions=["fenced_code", "tables", "toc", "codehilite",
                                       "sane_lists", "attr_list"],
                           extension_configs={"codehilite": {"guess_lang": False,
                                                             "css_class": "codehilite"}})
    html = md.convert(md_text)
    toc = md.toc_tokens

    # prefix every heading id with the doc key so cross-doc anchors don't collide
    ids = []
    def collect(tokens):
        for t in tokens:
            ids.append(t["id"])
            collect(t.get("children", []))
    collect(toc)
    for i in ids:
        html = html.replace(f'id="{i}"', f'id="{key}-{i}"')
    def reprefix(tokens):
        for t in tokens:
            t["id"] = f'{key}-{t["id"]}'
            reprefix(t.get("children", []))
    reprefix(toc)
    return html, toc


def nav_html(sources_toc):
    """Build the sidebar nav from each doc's toc (top-level = doc title, nested = h2/h3)."""
    out = ['<ul class="nav-root">']
    for key, title, toc in sources_toc:
        out.append(f'<li class="nav-doc"><a href="#{key}-top">{title}</a>')
        # children: use the doc's toc (skip the single H1, list its children)
        top = toc[0] if toc else None
        kids = top["children"] if top else []
        if kids:
            out.append('<ul>')
            for k in kids:
                out.append(f'<li><a href="#{k["id"]}">{k["name"]}</a>')
                if k.get("children"):
                    out.append('<ul>')
                    for gk in k["children"]:
                        out.append(f'<li><a href="#{gk["id"]}">{gk["name"]}</a></li>')
                    out.append('</ul>')
                out.append('</li>')
            out.append('</ul>')
        out.append('</li>')
    out.append('</ul>')
    return "\n".join(out)


CSS = """
:root{--accent:#2980b9;--side:#343131;--side2:#2c2c2c;--sidetx:#d9d9d9;--ink:#404040;--muted:#666;--codebg:#f7f7f9;--border:#e1e4e5;}
*{box-sizing:border-box;}
body{margin:0;font-family:'Lato',-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;color:var(--ink);line-height:1.6;}
a{color:var(--accent);text-decoration:none;} a:hover{text-decoration:underline;}
.wrap{display:flex;min-height:100vh;}
.side{width:300px;flex:0 0 300px;background:var(--side);color:var(--sidetx);position:sticky;top:0;height:100vh;overflow-y:auto;}
.side .brand{background:var(--side2);padding:18px 20px;font-size:1.05rem;font-weight:700;color:#fff;}
.side .brand small{display:block;font-weight:400;font-size:.72rem;color:#a3c7e0;margin-top:3px;}
.side input{width:calc(100% - 24px);margin:12px;padding:7px 9px;border:none;border-radius:3px;font-size:.85rem;}
.side nav{padding:6px 0 40px;}
.side ul{list-style:none;margin:0;padding:0;}
.side .nav-root>li{border-top:1px solid rgba(255,255,255,.06);}
.side .nav-doc>a{display:block;padding:9px 20px;color:#fff;font-weight:700;font-size:.9rem;}
.side ul ul a{display:block;padding:5px 20px 5px 30px;color:var(--sidetx);font-size:.83rem;}
.side ul ul ul a{padding-left:42px;color:#b7b7b7;font-size:.8rem;}
.side a:hover{background:rgba(255,255,255,.08);text-decoration:none;}
.side a.active{background:var(--accent);color:#fff;}
.main{flex:1 1 auto;min-width:0;}
.content{max-width:860px;margin:0 auto;padding:40px 48px 120px;}
.content h1{font-size:1.9rem;border-bottom:1px solid var(--border);padding-bottom:.3em;margin-top:0;}
.content h2{font-size:1.45rem;margin-top:1.8em;border-bottom:1px solid var(--border);padding-bottom:.2em;}
.content h3{font-size:1.15rem;margin-top:1.5em;}
.content code{background:var(--codebg);border:1px solid var(--border);border-radius:3px;padding:1px 5px;font-size:.86em;font-family:'SFMono-Regular',Consolas,Menlo,monospace;color:#e74c3c;}
.content pre{background:var(--codebg);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:3px;padding:12px 14px;overflow-x:auto;}
.content pre code{background:none;border:none;padding:0;color:#404040;font-size:.83rem;}
.content table{border-collapse:collapse;margin:1em 0;font-size:.9rem;display:block;overflow-x:auto;}
.content th,.content td{border:1px solid var(--border);padding:7px 11px;text-align:left;vertical-align:top;}
.content th{background:#f3f6f6;font-weight:700;}
.content tr:nth-child(even) td{background:#fafbfc;}
.content blockquote{border-left:3px solid var(--accent);margin:1em 0;padding:.3em 1em;color:var(--muted);background:#f8fafc;}
.doc-section{padding-top:10px;} .doc-section+.doc-section{border-top:2px solid var(--border);margin-top:40px;}
.gen{color:var(--muted);font-size:.8rem;margin-top:60px;border-top:1px solid var(--border);padding-top:14px;}
@media(max-width:820px){.side{display:none;}.content{padding:24px;}}
"""

JS = """
// scrollspy: highlight the sidebar link of the section in view
const links=[...document.querySelectorAll('.side a[href^="#"]')];
const map=new Map(links.map(a=>[a.getAttribute('href').slice(1),a]));
const targets=[...map.keys()].map(id=>document.getElementById(id)).filter(Boolean);
function onScroll(){let cur=null;for(const t of targets){if(t.getBoundingClientRect().top<120)cur=t.id;}
  links.forEach(a=>a.classList.remove('active'));if(cur&&map.get(cur))map.get(cur).classList.add('active');}
document.addEventListener('scroll',onScroll,{passive:true});onScroll();
// filter box
const box=document.getElementById('filter');
box&&box.addEventListener('input',()=>{const q=box.value.toLowerCase();
  document.querySelectorAll('.side nav li').forEach(li=>{const a=li.querySelector('a');
    li.style.display=(!q||(a&&a.textContent.toLowerCase().includes(q)))?'':'none';});});
"""


def build():
    sources_toc = []
    body_parts = []
    for key, title, path in SOURCES:
        if not os.path.exists(path):
            print(f"warning: missing {path}", file=sys.stderr); continue
        html, toc = convert(open(path, encoding="utf-8").read(), key)
        sources_toc.append((key, title, toc))
        body_parts.append(f'<section class="doc-section" id="{key}-top">\n{html}\n</section>')

    pyg = HtmlFormatter().get_style_defs(".codehilite")
    page = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Heat Transition Engine — Docs</title>
<link href="https://fonts.googleapis.com/css2?family=Lato:wght@400;700&display=swap" rel="stylesheet">
<style>{CSS}\n{pyg}</style></head>
<body><div class="wrap">
<aside class="side">
  <div class="brand">Heat Transition Engine<small>Java model — architecture &amp; parity</small></div>
  <input id="filter" type="text" placeholder="filter sections…">
  <nav>{nav_html(sources_toc)}</nav>
</aside>
<div class="main"><div class="content">
{''.join(body_parts)}
<p class="gen">Generated from the repo markdown by <code>docs/build_docs.py</code>. Regenerate after edits.</p>
</div></div>
</div><script>{JS}</script></body></html>"""
    open(OUT, "w", encoding="utf-8").write(page)
    print(f"wrote {OUT}  ({len(page)//1024} KB)")


if __name__ == "__main__":
    build()
