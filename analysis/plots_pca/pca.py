from collections import defaultdict
import csv
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import pandas as pd
import prince

db_path = "amazonianvoices.sqlite3"
output_csv = "segments.csv"

conn = sqlite3.connect(db_path)
cur = conn.cursor()
cur.execute("SELECT L.cldf_name as Language, l.Family, f.cldf_segments as cldf_segments FROM FormTable AS f, LanguageTable AS l WHERE f.cldf_languageReference == l.cldf_id")

# Get unique segments
lang_segment_counts = defaultdict(lambda: defaultdict(int))
all_segments = set()
lang_family = {}

import unicodedata

def strip_diacritics(text):
    normalized = unicodedata.normalize('NFKD', text)
    return ''.join(c for c in normalized if not unicodedata.combining(c))

for lang, fams, segments in cur.fetchall():
    lang_family[lang] = fams
    if not segments:
        continue
    
    phones = segments.strip().split()
    
    for p in phones:
        p = strip_diacritics(p)
        lang_segment_counts[lang][p] += 1
        all_segments.add(p)

# Sort segments and langs
all_segments = sorted(all_segments)
languages = sorted(lang_segment_counts.keys())

# Step 3: write CSV
with open(output_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["Language", "Family"] + all_segments)
    
    for lang in languages:
        row = [lang, lang_family.get(lang, "")]

        for seg in all_segments:
            row.append(lang_segment_counts[lang].get(seg, 0))

        writer.writerow(row)

conn.close()

# Load data as pandas
df = pd.read_csv("segments.csv").set_index(["Language", "Family"])
# print(df)

# fit PCA
pca = prince.PCA(
    n_components=3,
    n_iter=3,
    rescale_with_mean=True,
    rescale_with_std=True,
    copy=True,
    check_input=True,
    engine='sklearn',
    random_state=42
)

pca = pca.fit(df)

coords = pca.row_coordinates(df)

# Create plot
plt.figure(figsize=(10, 7))

sns.scatterplot(
    data=coords,
    x=0,
    y=1,
    hue="Family",
    edgecolor="black",
    linewidth=0.7,
    s=100,
    alpha=0.8
)

# Remove labels for better visibility
unlabel = [
    "Marinawa", "Shawandawá","Cashinahua", "Kakataibo Sinchi Roca", "Matses", "Iskonahua", "Chaninawa",

    "Yanesha", "Matsigenka", "Awajún", "Ticuna", "Shawi", "Pastaza Kichwa"
]

import matplotlib.patheffects as pe

for i, txt in enumerate(coords.index):
    lang = str(txt[0])
    if lang not in unlabel:
        lang = " " + lang
        plt.text(
            coords.iloc[i, 0],
            coords.iloc[i, 1],
            lang,
            fontsize=13,
            alpha=1,
            path_effects=[
                pe.Stroke(linewidth=3, foreground="white"),
                pe.Normal()
            ]
        )

plt.xlabel("PC1")
plt.ylabel("PC2")
plt.tight_layout()
# plt.show()

plt.savefig("pca_count.png", dpi=500, bbox_inches="tight")
