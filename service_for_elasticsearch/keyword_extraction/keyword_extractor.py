from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
from nltk.stem import PorterStemmer, WordNetLemmatizer
from collections import defaultdict, Counter
from nltk.tag import pos_tag
from nltk.tokenize import word_tokenize
from langdetect import detect_langs
from sklearn.preprocessing import MinMaxScaler
import lemminflect
import re
import spacy
from spacy.tokenizer import Tokenizer
import fitz
import os
import numpy as np
import math

spacy_nlp = spacy.load('en_core_web_sm')
spacy_nlp_ru = spacy.load('ru_core_news_sm')
spacy_nlp.tokenizer = Tokenizer(spacy_nlp.vocab, token_match=re.compile(r'\S+').match)
spacy_nlp_ru.tokenizer = Tokenizer(spacy_nlp_ru.vocab, token_match=re.compile(r'\S+').match)
sw_s_en = stopwords.words('english')
sw_s_en.extend(['', 'et', 'al', 'none'])
check_en_pattern = r'[a-zA-Z]+'
sw_s_ru = stopwords.words('russian')
sw_s_ru.extend(['', 'др'])
check_ru_pattern = r'[а-яА-Я]+'
et_al_pattern = r'.*et al.*'
et_al_pattern_ru = r'.*и др.*'
sub_pattern = r'^[^\w]*(.*?)[^\w]*$'

def decontracted(phrase):
    # specific
    phrase = re.sub(r"won[\'\’]t", "will not", phrase)
    phrase = re.sub(r"can[\'\’]t", "can not", phrase)

    # general
    phrase = re.sub(r"n[\'\’]t", " not", phrase)
    phrase = re.sub(r"[\'\’]re", " are", phrase)
    phrase = re.sub(r"[\'\’]s", " is", phrase)
    phrase = re.sub(r"[\'\’]d", " would", phrase)
    phrase = re.sub(r"[\'\’]ll", " will", phrase)
    phrase = re.sub(r"[\'\’]t", " not", phrase)
    phrase = re.sub(r"[\'\’]ve", " have", phrase)
    phrase = re.sub(r"[\'\’]m", " am", phrase)
    return phrase

def is_noun(token, lang='en'):
    if lang == 'en':
        return token.tag_.startswith('NN') and token.pos_ not in ['PRON', 'ADJ']
    elif lang == 'ru':
        return 'NOUN' in token.tag_


def extract(URL, doc_text):
    compound_nouns = defaultdict(int)
    list_of_counters = {'tokens' : defaultdict(int), 'compounds' : []}
    list_of_counters['tokens'] = defaultdict(int)

    decontracted_text = decontracted(doc_text)
    
    if str(detect_langs(doc_text)[0]).startswith('ru') or str(detect_langs(doc_text)[0]).startswith('bg'):
        spacy_nlp_ru.max_length = len(decontracted_text)
        doc = spacy_nlp_ru(decontracted_text)
        lang = 'ru'
    else:
        spacy_nlp.max_length = len(decontracted_text)
        doc = spacy_nlp(decontracted_text)
        lang = 'en'
        
    i = 0
        
    while i < len(doc):

        if lang == 'ru':
            if doc[i].dep_ in ["amod"]:
                # print(doc[i].text, texts[i])
                comp_text = re.sub(sub_pattern, r'\1',
                                doc[i].text.strip())
                if doc[i+1].dep_.startswith("nsubj"):
                    # print(doc[i+1].text, doc[i+1].dep_)
                    comp_text += ' ' + \
                        re.sub(sub_pattern, r'\1',
                            doc[i+1].text.strip())

                if ' ' in comp_text.strip() and comp_text.strip() and not re.compile(et_al_pattern_ru).match(comp_text.strip()) and not all(token.strip().lower() in sw_s_ru for token in comp_text.split()):
                    compound_nouns[comp_text.strip().title()] += 1
                    i += 1
                    continue

        else:
            if doc[i].dep_ in ["amod", "compound"]:
                comp_text = re.sub(sub_pattern, r'\1',
                                doc[i].text.strip())

                while doc[i+1].dep_ == "compound":
                    i += 1
                    # print(doc[i].text, doc[i].dep_)
                    comp_text += ' ' + \
                        re.sub(sub_pattern, r'\1',
                            doc[i].text.strip())
                if doc[i].dep_ == 'compound' and doc[i+1].dep_ not in ['ROOT', 'appos', 'nmod']:
                    # print(doc[i+1].text, doc[i+1].dep_)
                    comp_text += ' ' + \
                        re.sub(sub_pattern, r'\1',
                            doc[i+1].text.strip())

                if ' ' in comp_text.strip() and comp_text.strip() and not re.compile(et_al_pattern).match(comp_text.strip()) and not all(token.strip().lower() in sw_s_en for token in comp_text.split()):
                    compound_nouns[comp_text.strip().title()] += 1
                    i += 1
                    continue

        token = re.sub(sub_pattern, r'\1', doc[i]._.lemma().strip())
        if token:
            if re.compile(check_en_pattern).match(token):
                if token.lower() not in sw_s_en and is_noun(doc[i], lang):
                    list_of_counters['tokens'][token.title()] += 1

            elif re.compile(check_ru_pattern).match(token):
                if token.lower() not in sw_s_ru and is_noun(doc[i], lang):
                    list_of_counters['tokens'][re.sub(
                        sub_pattern, r'\1', doc[i].lemma_.strip().title())] += 1
            else:
                if token.lower() not in sw_s_en and is_noun(doc[i], lang):
                    list_of_counters['tokens'][token.title()] += 1

        i += 1

    list_of_counters['compounds'] = sorted(
        compound_nouns.items(), key=lambda x: x[1], reverse=True)
        
    all_keys = []
    scaler = MinMaxScaler()

    sorted_keys = sorted([items for items in list_of_counters['tokens'].items(
    ) if len(items[0]) > 1], key=lambda x: x[1], reverse=True)

    sorted_compounds = sorted(
        list_of_counters['compounds'], key=lambda x: x[1], reverse=True)
    
    
    if sorted_keys:
        scaled_keys = scaler.fit_transform(
            np.array([item[1] for item in sorted_keys]).reshape(-1, 1))
    else: scaled_keys = np.array([])
    
    if sorted_compounds:
        scaled_compounds = scaler.fit_transform(
            np.array([item[1] for item in sorted_compounds]).reshape(-1, 1))
    else: scaled_compounds = np.array([])
    
    for i in range(len(sorted_keys)):
        if scaled_keys[i] < 0.5:
            break
    else:
        i = 0
    for j in range(len(sorted_compounds)):
        if scaled_compounds[j] < 0.5:
            break
    else:
        j = 0
    
    all_keys = [{'name' : item[0], 'score' : scaled_keys[:i][idx_i][0]} for idx_i, item in enumerate(sorted_keys[:i])] + [{'name' : item[0], 'score' : scaled_compounds[:j][idx_j][0]} for idx_j, item in enumerate(sorted_compounds[:j])]

    return all_keys
