# Updating AmazonianVoices

- download new data into raw/data/<lg>
- update language metadata in `etc/languages.csv`
- run `cldfbench download ...` to convert into raw/csv/<lg_id> with files
  - `data.csv`
  - `audio/`
- run `upload.py` to upload audio to CDSTAR, updating `raw/catalog.json`.

Then it should be possible to run

```shell
cldfbench lexibank.makecldf lexibank_amazonianvoices.py --glottolog ../../glottolog/glottolog
```
