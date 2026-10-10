"""Build SUBMISSION.pdf from SUBMISSION.md with headless Chrome.
Needs the `markdown` package and Chrome. Only used to produce the PDF; the
pipeline itself needs neither.   python tools/build_pdf.py"""
import os
import re
import subprocess
import markdown

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHROME = os.environ.get("CHROME", r"C:\Program Files\Google\Chrome\Application\chrome.exe")

md = open(os.path.join(ROOT, "SUBMISSION.md"), encoding="utf-8").read()
mermaid = []


def keep(m):
    mermaid.append(m.group(1))
    return f"MERMAIDBLOCK{len(mermaid) - 1}"


md = re.sub(r"```mermaid\n(.*?)```", keep, md, flags=re.S)
body = markdown.markdown(md, extensions=["tables", "fenced_code"])
for i, src in enumerate(mermaid):
    body = body.replace(f"<p>MERMAIDBLOCK{i}</p>", f'<pre class="mermaid">{src}</pre>')

html = f"""<!doctype html><html><head><meta charset="utf-8"><title>Engineer 004 - Raj Lakhani</title>
<style>
@page {{ size: Letter; margin: 0.5in 0.55in; }}
body {{ font-family: Inter, Segoe UI, Arial, sans-serif; font-size: 9.2pt; line-height: 1.3; color: #171717; }}
h1 {{ font-size: 16pt; margin: 0 0 4pt; }}
h2 {{ font-size: 12pt; margin: 9pt 0 3pt; border-bottom: 1px solid #ddd; padding-bottom: 2pt; }}
h3 {{ font-size: 10.6pt; margin: 7pt 0 2pt; }}
p {{ margin: 2pt 0 4pt; }}
table {{ border-collapse: collapse; width: 100%; margin: 3pt 0 6pt; font-size: 8.3pt; }}
th, td {{ border: 1px solid #d4d4d4; padding: 2pt 3.5pt; vertical-align: top; text-align: left; }}
th {{ background: #f4f4f4; }}
code {{ font-family: Consolas, monospace; font-size: 8.4pt; background: #f4f4f4; padding: 0 2px; word-break: break-all; }}
pre {{ background: #f6f6f6; padding: 5pt; font-size: 8.2pt; white-space: pre-wrap; }}
pre.mermaid {{ background: none; text-align: center; margin: 0; }}
pre.mermaid svg {{ max-height: 2.0in; }}
a {{ color: #0b57d0; }}
ul {{ margin: 2pt 0 5pt 16pt; padding: 0; }}
</style>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<script>mermaid.initialize({{ startOnLoad: true, theme: "neutral", flowchart: {{ useMaxWidth: true }} }});</script>
</head><body>{body}</body></html>"""
out_html = os.path.join(ROOT, ".data", "SUBMISSION.html")
os.makedirs(os.path.dirname(out_html), exist_ok=True)
open(out_html, "w", encoding="utf-8").write(html)
pdf = os.path.join(ROOT, "SUBMISSION.pdf")
subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", "--virtual-time-budget=8000",
                f"--print-to-pdf={pdf}", "file:///" + out_html.replace("\\", "/")], check=True,
               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
print("wrote", pdf)
