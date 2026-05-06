extensions = [
    "myst_parser",
    "sphinx.ext.githubpages",
    "sphinxcontrib.openapi",
    "sphinx_rtd_theme",
]

root_doc = "index"
html_theme = "sphinx_rtd_theme"

project = "SkyDriver"
author = "WIPAC Developers"
html_show_copyright = False

html_title = "SkyDriver"
html_short_title = "SkyDriver"

html_last_updated_fmt = "%Y-%m-%d %H:%M UTC"
html_last_updated_use_utc = True

exclude_patterns = [
    "_build",
]

html_static_path = ["_static"]
html_css_files = [
    "skydriver.css",
]
