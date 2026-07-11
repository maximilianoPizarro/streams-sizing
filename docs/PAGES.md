# GitHub Pages

This site is published from the `/docs` folder on branch `main`.

## Enable Pages

1. Open **Settings → Pages** in the GitHub repository.
2. **Build and deployment → Source:** Deploy from a branch.
3. **Branch:** `main` / **Folder:** `/docs`
4. Save. The site will be available at:

   `https://maximilianopizarro.github.io/streams-sizing/`

## Configuration

- [`_config.yml`](_config.yml): `baseurl: "/streams-sizing"` must match the repository name.
- Calculator uses ES modules; serve over HTTP (Pages or `jekyll serve`).

## Local build

```bash
cd docs
bundle install
bundle exec jekyll serve --baseurl ""
```

Open `http://127.0.0.1:4000/`.
