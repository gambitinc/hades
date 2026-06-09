FROM python:3.12-alpine
WORKDIR /srv
# whole context: index.html, docs.html, install.sh, serve.py, source tarball
COPY . ./
EXPOSE 8000
CMD ["python", "serve.py"]
