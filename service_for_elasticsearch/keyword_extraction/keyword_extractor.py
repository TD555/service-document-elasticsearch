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
sw_s_en.extend(['', 'et', 'al', 'none', 'true', 'false'])
check_en_pattern = r'[a-zA-Z]+'
sw_s_ru = stopwords.words('russian')
sw_s_ru.extend(['', 'др'])
check_ru_pattern = r'[а-яА-Я]+'
et_al_pattern = r'.*et al.*'
et_al_pattern_ru = r'.*и др.*'
keywords_pattern_en = r'K[Ee][Yy]\s*[Ww][Oo][Rr][Dd][Ss]\W*([ [][^\n]*)'
sub_pattern = r'^[^\w]*(.*?)[^)\w]*$'
word_pattern = r'[^\W&&[^0-9]+'

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

def is_not_month(value):
    months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
    return value not in months


def is_noun(token, lang='en'):
    if lang == 'en':
        return token.tag_.startswith('NN') and token.pos_ not in ['PRON', 'ADJ'] and is_not_month(token.text.strip())
    elif lang == 'ru':
        return 'NOUN' in token.tag_ 


def extract(doc_text, check_keywords=True):
    compound_nouns = defaultdict(int)
    list_of_counters = {'tokens' : defaultdict(int), 'compounds' : []}
    list_of_counters['tokens'] = defaultdict(int)

    clean_text = re.sub(r'\s+', ' ', doc_text).replace('\n', ' ')
    decontracted_text = decontracted(clean_text)
    
    try:
        if str(detect_langs(doc_text)[0]).startswith('ru') or str(detect_langs(doc_text)[0]).startswith('bg'):
            spacy_nlp_ru.max_length = len(decontracted_text)
            doc = spacy_nlp_ru(decontracted_text)
            lang = 'ru'
            all_keys = []
        else:
            spacy_nlp.max_length = len(decontracted_text)
            doc = spacy_nlp(decontracted_text)
            lang = 'en'
            if check_keywords:
                match = re.search(keywords_pattern_en, doc_text)
                if match and match.group(1) is not None:
                    if ',' in match.group(1):
                        all_keys = [{'name' : key.strip().title(), 'score' : 10.0 } for key in match.group(1).split(',')[:] if key.strip().title()]
                    elif ';' in match.group(1):
                        all_keys = [{'name' : key.strip().title(), 'score' : 10.0 } for key in match.group(1).split(';')[:] if key.strip().title()]
                    elif '·' in match.group(1):
                        all_keys = [{'name' : key.strip().title(), 'score' : 10.0 } for key in match.group(1).split('·')[:] if key.strip().title()]
                    else: 
                        all_keys = []
                else: 
                    all_keys = []
            
            else: all_keys = []
                
    except Exception as e: 
        # spacy_nlp.max_length = len(decontracted_text)
        doc = spacy_nlp(decontracted_text)
        lang = 'en'
        
    i = 0
    
    while i < len(doc):
        
        if lang == 'ru':
            if doc[i].dep_ in ["amod"]:
                comp_text = re.sub(sub_pattern, r'\1',
                                doc[i].text.strip())
                if i+1 < len(doc) and doc[i+1].dep_.startswith("nsubj"):
                    # print(doc[i+1].text, doc[i+1].dep_)
                    comp_text += ' ' + \
                        re.sub(sub_pattern, r'\1',
                            doc[i+1].text.strip())

                if ' ' in comp_text.strip() and comp_text.strip() and not re.compile(et_al_pattern_ru).match(comp_text.strip()) and not any(token.strip().lower() in sw_s_ru for token in comp_text.split()) and all(len(token.strip()) > 1 for token in comp_text.split()):
                    compound_nouns[comp_text.strip().title()] += 1
                    i += 1
                    continue

        else:
            if doc[i].dep_ in ["amod", "compound"] and is_not_month(doc[i].text.strip()):
                comp_text = re.sub(sub_pattern, r'\1',
                                doc[i].text.strip())
                
                while i+1 < len(doc) and doc[i+1].dep_ == "compound" and is_not_month(doc[i+1].text.strip()):
                    i += 1
                    # print(doc[i].text, doc[i].dep_)
                    comp_text += ' ' + \
                        re.sub(sub_pattern, r'\1',
                            doc[i].text.strip())
                if i+1 < len(doc) and doc[i].dep_ == 'compound' and doc[i+1].dep_ not in ['ROOT', 'appos', 'nmod'] and is_not_month(doc[i+1].text.strip()):
                    # print(doc[i+1].text, doc[i+1].dep_)
                    comp_text += ' ' + \
                        re.sub(sub_pattern, r'\1',
                            doc[i+1].text.strip())

                if ' ' in comp_text.strip() and comp_text.strip() and re.compile(word_pattern).match(comp_text.strip()) and not re.compile(et_al_pattern).match(comp_text.strip()) and not any(token.strip().lower() in sw_s_en for token in comp_text.split()) and all(len(token.strip()) > 1 for token in comp_text.split()):
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
                    
            elif re.compile(word_pattern).match(token):
                if token.lower() not in sw_s_en and is_noun(doc[i], lang):
                    list_of_counters['tokens'][token.title()] += 1

        i += 1

    list_of_counters['compounds'] = sorted(
        compound_nouns.items(), key=lambda x: x[1], reverse=True)
        
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
    
    all_keys.extend([{'name' : item[0], 'score' : scaled_keys[:i][idx_i][0] * 18 - 8} for idx_i, item in enumerate(sorted_keys[:i]) if item[0] and item[0].lower() not in [key['name'].lower() for key in all_keys]] + 
                     [{'name' : item[0], 'score' : scaled_compounds[:j][idx_j][0] * 18 - 8} for idx_j, item in enumerate(sorted_compounds[:j]) if  item[0] and item[0].lower() not in [key['name'].lower() for key in all_keys]])

    return all_keys[:20]
