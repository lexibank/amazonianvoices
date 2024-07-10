import attr
import openpyxl
import pydub
import re
import shutil
import unicodedata

import csv
from csvw.metadata import URITemplate
import pylexibank

from clldutils.path import Path
from clldutils.misc import slug


@attr.s
class CustomConcept(pylexibank.Concept):
    Spanish_Gloss = attr.ib(default=None)
    Scientific_Name = attr.ib(default=None)
    Concepticon_SemanticField = attr.ib(default=None)


class Dataset(pylexibank.Dataset):
    dir = Path(__file__).parent
    id = "amazonvoices"

    concept_class = CustomConcept

    form_spec = pylexibank.FormSpec(
        missing_data=['-'],
        replacements=[
            (':', 'ː'),
        ],
        normalize_unicode='NFC',
    )

    def cmd_download(self, args):
        """
        - converts ./raw/data/*.{wav,xlsx} into ./raw/csv/*.{lg_id/data.csv,lg_id/audio/*.wav}
        - all raw material (xlsx,wav) from Google Drive must be downloaded into ./raw/data first
        """

        def norm(x):
            if x is None:
                return None
            return unicodedata.normalize('NFC', unicodedata.normalize('NFD', x))

        def get_audio_id(d):
            aid = ''
            if 'audio_id' in d:
                aid = d['audio_id'].strip()
            elif 'audio_file' in d:
                aid = d['audio_file'].strip()
            if aid and aid != '-' and aid != '?':
                return aid
            return None

        def get_concept_id(d):
            cid = d['concept-id'].replace('cbr_sr', 'cbrsr').strip()
            if cid == '':
                return None
            ids = cid.split('_')[1:]
            return '_'.join(map(lambda x: str(int(x)), ids))

        def get_first_dir(p):
            for f in p.iterdir():
                if f.is_dir() and 'audio' in f.stem.lower():
                    return f
            return None

        def get_audio_filename_via_slug(p, w):
            all_wavs = {}
            for wav in p.glob('**/*.wav'):
                wav_s = slug(wav.stem)
                if wav_s in all_wavs:
                    return None
                all_wavs[wav_s] = wav.stem
            ws = slug(w)
            if ws in all_wavs:
                return all_wavs[ws]
            return None

        valid_lg_ids = [lg['ID'] for lg in self.languages]
        valid_param_ids = [c['ID'] for c in self.concepts]

        data_header = ['param_id', 'form', 'audio']

        datadir = self.raw_dir / 'data'
        for f in sorted(datadir.iterdir()):
            if f.is_dir() and f.stem.isupper():
                lg_id = slug(f.stem)
                if lg_id not in valid_lg_ids:
                    continue
                data = []
                shutil.rmtree(self.raw_dir / 'csv' / lg_id / 'audio', ignore_errors=True)
                for xlsx in sorted(f.glob('**/*.xlsx')):
                    if 'concept' in xlsx.stem.lower() and '/~' not in str(xlsx):
                        wb = openpyxl.load_workbook(xlsx, data_only=True)
                        for sname in wb.sheetnames:
                            if sname.lower() == 'concepts':
                                sheet = wb[sname]
                                last_cid = None
                                for i, row in enumerate(sheet.rows):
                                    row = ['' if col.value is None else '{0}'.format(col.value).strip() for col in row]
                                    if i == 0:
                                        header = list(map(str.lower, row))
                                    else:
                                        assert header
                                        d = dict(zip(header, row))
                                        word = d['segment'].strip()
                                        cid = get_concept_id(d)
                                        if cid is None:
                                            if word and last_cid is not None:
                                                cid = last_cid
                                            else:
                                                continue
                                        last_cid = cid
                                        if cid not in valid_param_ids:
                                            continue
                                        if word == '' or word == '-':
                                            continue
                                        aid = norm(get_audio_id(d))
                                        audio_path = None
                                        audio_name = ''
                                        if aid:
                                            aid = re.sub(r'\.(wav)?$', '', aid)
                                            audio_dir = get_first_dir(f)
                                            if '/' in aid:
                                                p, asid = aid.split('/')
                                                fp = audio_dir / p / f'{norm(asid.strip())}.wav'
                                                if fp.exists() and fp.is_file():
                                                    audio_path = fp
                                            if audio_path is None:
                                                fp = audio_dir / f'{norm(aid)}.wav'
                                                if fp.exists() and fp.is_file():
                                                    audio_path = fp
                                            if audio_path is None:
                                                n = get_audio_filename_via_slug(audio_dir, aid)
                                                fp = audio_dir / f'{n}.wav'
                                                if fp.exists() and fp.is_file():
                                                    audio_path = fp
                                                    # args.log.info(f'found heuristically {f.stem} {cid} {aid}')
                                            if audio_path is None:
                                                audio_path = ''
                                                args.log.info(f'audio missing for {f.stem} {cid} {word} "{get_audio_id(d)}"')
                                            else:
                                                ap = self.raw_dir / 'csv' / lg_id / 'audio'
                                                ap.mkdir(exist_ok=True)
                                                wav = pydub.AudioSegment.from_file(str(audio_path), format='wav')
                                                if wav.channels > 1:
                                                    wav_o = wav.split_to_mono()[0]
                                                    if wav_o.rms < 10:
                                                        wav_o = wav.split_to_mono()[1]
                                                    if wav.rms < 10:
                                                        args.log.warning(f'check audio for {f.stem} {cid} {word} "{get_audio_id(d)}"')
                                                else:
                                                    wav_o = wav
                                                n = 1
                                                fp = ap / f'{cid}__{n}.wav'
                                                while fp.exists():
                                                    n += 1
                                                    fp = ap / f'{cid}__{n}.wav'
                                                audio_name = f'{cid}__{n}.wav'
                                                wav_o.export(str(fp), format='wav', codec='copy')
                                        data.append([cid, word, audio_name])
                                break

                        cdir = self.raw_dir / 'csv' / lg_id
                        cdir.mkdir(exist_ok=True)
                        with csvw.UnicodeWriter(cdir / 'data.csv') as w:
                            w.writerow(data_header)
                            w.writerows(sorted(data, key=(lambda i: int(i[0].split('_')[0]))))

    def cmd_makecldf(self, args):

        with args.writer as ds:

            for c in self.concepts:
                ds.add_concept(**c)

            valid_lang_ids = set()
            for lg in self.languages:
                valid_lang_ids.add(lg['ID'])
                ds.add_language(**lg)

            ds.cldf.add_component(
                'MediaTable',
                'objid',
                {'name': 'size', 'datatype': 'integer'},
                {
                    'name': 'Form_ID',
                    'required': True,
                    'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#formReference',
                    'datatype': 'string'
                },
                {
                    'name': 'mimetype',
                    'required': True,
                    'datatype': {'base': 'string', 'format': '[^/]+/.+'}
                },
            )
            ds.cldf.remove_columns('MediaTable', 'Download_URL')
            ds.cldf.remove_columns('MediaTable', 'Description')
            ds.cldf.remove_columns('MediaTable', 'Path_In_Zip')
            ds.cldf.remove_columns('MediaTable', 'Media_Type')
            ds.cldf['MediaTable', 'ID'].valueUrl = URITemplate('https://cdstar.eva.mpg.de/bitstreams/{objid}/{Name}')
            ds.cldf['MediaTable', 'mimetype'].propertyUrl = URITemplate('http://cldf.clld.org/v1.0/terms.rdf#mediaType')

            sound_cat = self.raw_dir.read_json('catalog.json')
            sound_map = dict()
            for k, v in sound_cat.items():
                sound_map[v['metadata']['name']] = k

            for lang_dir in pylexibank.progressbar(
                    sorted((self.raw_dir / 'csv').iterdir(), key=lambda f: f.name),
                    desc="adding new data"):

                if not lang_dir.is_dir():
                    continue

                lang_id = lang_dir.name

                if lang_id not in valid_lang_ids:
                    continue

                with open(lang_dir / 'data.csv') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        new = ds.add_form(
                            Language_ID=lang_id,
                            Local_ID='',
                            Parameter_ID=row['param_id'],
                            Value=row['form'],
                            Form=self.form_spec.clean(row['form']),
                        )
                        if row['audio']:
                            if row['audio'] in sound_map:
                                for bs in sorted(sound_cat[sound_map[media_id]]['bitstreams'],
                                                 key=lambda x: x['content-type']):
                                    ds.objects['MediaTable'].append({
                                        'ID': bs['checksum'],
                                        'Name': bs['bitstreamid'],
                                        'objid': sound_map[media_id],
                                        'mimetype': bs['content-type'],
                                        'size': bs['filesize'],
                                        'Form_ID': new['ID'],
                                    })
                            else:
                                args.log.warning(f'audio file {row["audio"]} not found in catalog')
