# My Analytics Website

Personal analytics blog built with [Quarto](https://quarto.org) and hosted on GitHub Pages.
Below is an outline of how to get started if you want to clone and create your own.

## Tech Stack
- **Quarto** – site framework and document rendering
- **R** – statistical analysis and ggplot2 visualisations
- **Python** – data processing and Plotly charts
- **GitHub Pages** – free hosting via the `/docs` output folder
- **GitHub Actions** – auto-renders and deploys on every push to `main`

## Local Development

### Prerequisites
- [Quarto](https://quarto.org/docs/get-started/) installed
- R + required packages: `tidyverse`, `plotly`, `lubridate`, `scales`
- Python + required packages: `pandas`, `numpy`, `plotly`

### Run locally
```bash
quarto preview
```
This starts a local server at `http://localhost:4444` with live reload.

### Render to /docs
```bash
quarto render
```

## Project Structure
```
website/
├── _quarto.yml                        ← site configuration
├── index.qmd                          ← landing page with post grid
├── about.qmd                          ← about page
├── styles.css                         ← custom styles
├── .github/workflows/publish.yml      ← auto-deploy action
└── posts/
    ├── sa-load-shedding/
    │   ├── index.qmd
    │   └── thumbnail.png
    ├── housing-market-analysis/
    │   ├── index.qmd
    │   └── thumbnail.png
    └── python-inflation-tracker/
        ├── index.qmd
        └── thumbnail.png
```

## Adding a New Post
1. Create a new folder under `posts/your-post-name/`
2. Add `index.qmd` with YAML front matter (title, date, categories, image)
3. Add a `thumbnail.png` (any image ~800×400px works well)
4. Push to `main` — GitHub Actions renders and deploys automatically
