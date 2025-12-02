from jinja2 import Environment, FileSystemLoader
from pathlib import Path

def build_report(results, output_file="report.html"):
    env = Environment(loader=FileSystemLoader("overseer/reporting/templates"))
    template = env.get_template("report.html")
    html = template.render(results=results)
    Path(output_file).write_text(html, encoding="utf-8")
    print(f"Report saved to {output_file}")
