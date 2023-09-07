from flask import Flask, jsonify, request, json, abort, render_template
from elasticsearch import Elasticsearch, ConnectionError , BadRequestError, exceptions
import asyncio
from datetime import datetime
import requests
from collections import defaultdict
import tempfile
import uuid
import traceback
import time
import pytz
 

from pdfminer.converter import TextConverter
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfpage import PDFPage
import os
import re
import io

from version import __version__, __description__

app = Flask(__name__)

# API_KEY = "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"
# MODEL = "gpt-3.5-turbo"

# URL = "http://192.168.0.176:5000"
INDEX = 'my_index'

es_host = os.environ['ELASTICSEARCH_URL']
# es_host = "http://localhost:9201/"

es = Elasticsearch([es_host]) 

request_timeout = 20
upload_timeout = 40

gmt_plus_4 = pytz.timezone('Asia/Dubai')


put_data = {
  "settings": {
    "analysis": {
      "analyzer": {
        "my_analyzer": {
          "tokenizer": "my_tokenizer",
          "filter": ["lowercase"]
        }
      },
      "tokenizer": {
        "my_tokenizer": {
          "type": "pattern",
          "pattern": "[ ]+"

        }
      }
    }
  },
  "mappings": {
    "properties": {
      "created": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "page_content" : {
          "type": "text",
          "analyzer" : "my_analyzer",
      }
    }
  }
}



try:
    es.indices.create(index='my_index', body=put_data)
    
except BadRequestError as e: 
    # print(str(e))
    pass


    
async def get_filestorage_object(url):
    response = await asyncio.get_event_loop().run_in_executor(None, requests.get, url)
    if response.status_code == 200:
        file_object = io.BytesIO(response.content)
        return file_object
    else:
        # Handle error cases
        return None

def remove_duplicates(input_list):
    seen = set()
    unique_list = []
    
    for item in input_list:
        frozen_item = frozenset(item.items())
        
        if frozen_item not in seen:
            seen.add(frozen_item)
            unique_list.append(item)
    
    return unique_list

def get_context(output_stream, interpreter, all_texts, all_pages):
    for page in all_pages:

        interpreter.process_page(page)

        text = output_stream.getvalue()

        print(text)
        # do something with the text for this page
        text = text.replace("\n", " ")
        all_texts.append(text)

        # reset the output stream
        output_stream.seek(0)
        output_stream.truncate(0)


async def extract_text_from_pdf(pdf_file):
    
    print("start")
    # create a PDF resource manager and converter
    resource_manager = PDFResourceManager()
    output_stream = io.StringIO()
    converter = TextConverter(resource_manager, output_stream, laparams=None)
    # create a PDF interpreter and open the PDF file
    interpreter = PDFPageInterpreter(resource_manager, converter)
    all_texts = []
    # iterate over each page in the PDF file
    all_pages = PDFPage.get_pages(pdf_file)
    
    await asyncio.get_event_loop().run_in_executor(None, get_context, output_stream, interpreter, all_texts, all_pages)
    

    # cleanup
    converter.close()
    output_stream.close()
    
    return all_texts


import subprocess

async def extract_text_from_doc(doc_file):

    doc_file.seek(0)

    document_content = io.BytesIO(doc_file.read())
    temp_file = tempfile.NamedTemporaryFile(suffix='.docx')

    temp_file.write(document_content.getvalue())
    document_path = temp_file.name
    


    try:
        pdf_bytes = subprocess.check_output(["unoconv", "-f", "pdf", "--stdout", document_path])

    except subprocess.CalledProcessError as e:
        return {"message": e.stderr}

    # Create a BytesIO object from the PDF content
    pdf_stream = io.BytesIO(pdf_bytes)

    all_texts = await extract_text_from_pdf(pdf_stream)
    temp_file.close()
    
    return all_texts


async def extract_text_from_ppt(ppt_file):
    
    ppt_file.seek(0)
    
    ppt_content = io.BytesIO(ppt_file.read())
    temp_file = tempfile.NamedTemporaryFile(suffix='.pptx')
    temp_file.write(ppt_content.getvalue())
    ppt_path = temp_file.name

    try:
        pdf_bytes = subprocess.check_output(["unoconv", "-f", "pdf", "--stdout", ppt_path])
    except subprocess.CalledProcessError as e:
        return {"message": e.stderr}

    pdf_stream = io.BytesIO(pdf_bytes)

    all_texts = await extract_text_from_pdf(pdf_stream)

    temp_file.close()

    return all_texts
    
    
    

@app.errorhandler(Exception)
def handle_error(error):
    # Get the traceback
    error_traceback = traceback.format_exc()
    print(error_traceback)
    if hasattr(error, 'code'):
        status_code = error.code
    else:
        status_code = 500
    return {"message" : str(error), "status_code" : status_code}, status_code
    
    
@app.after_request
def after_request(response):
  response.headers.set('Access-Control-Allow-Origin', '*')
  response.headers.set('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.set('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response 


@app.route("/", methods=["GET"])
def info():
    return __description__


@app.route("/create_or_update", methods=["POST"])
async def create_or_update():

#   ---Get file and parse content---
    data_dict = {}
    
    try:
        nodes_data = request.json['nodes_data']
        data_dict['project_id'] = request.json['project_id']
        data_dict['user_id'] = request.json['user_id']
        data_dict['node_id'] = request.json['node_id']
        data_dict['node_name'] = request.json['node_name']
        data_dict['type_id'] = request.json['type_id']
        data_dict['property_id'] = request.json['property_id']
        data_dict['type_name'] = request.json['type']
        data_dict['property_name'] = request.json['property']
        data_dict['color'] = request.json['color']
        data_dict['default_image'] = request.json['default_image']
        
    except:
        abort(403, "Invalid raw data")
    
    all_docs = await get_list()
    filenames = list(set([item['path'] for item in all_docs.json['docs'] if item['node_id'] == data_dict['node_id']]))
    
    print("All filenames in the start", filenames)   
    data_dict['filenames'] = filenames
    
    returned_jsons = []
    for item in remove_duplicates(nodes_data):
        result = {**data_dict, **item}      
        returned_jsons.append(upload_document(result))
    
    print(returned_jsons)
    returned_jsons = await asyncio.gather(*returned_jsons)
    
    print("All filenames in the end", filenames)   
       
    my_namespace = uuid.NAMESPACE_DNS

    for filename in filenames:
        my_uuid = uuid.uuid5(my_namespace, filename + data_dict['node_id']) 
        old_id = str(my_uuid)
        delete_response = await delete(old_id, filename)
        returned_jsons.append(delete_response)      
    
    print((await get_list()).json['docs'])
    return jsonify({"messages" : returned_jsons, "status" : 200})
    
    
async def upload_document(data):    
    # parsed = parser.from_buffer(file.read())
    # text = parsed["content"]
    # content = text.strip()

    try:
        name= data['name']
        path = data['url']
    except: return {'message' : f"Invalid key names in nodes_data"}
    
    try:
        start = time.time()
        file = await asyncio.wait_for(get_filestorage_object(path), timeout=request_timeout)
        end = time.time()
        request_time = end - start
        
    except Exception: 
        return {'message' : f"Document reading timeout",  "URL" : path}
        
    project_id = data['project_id']
    user_id = data['user_id']
    node_id = data['node_id']
    node_name = data['node_name']
    type_id = data['type_id']
    property_id = data['property_id']
    type_name = data['type_name']
    property_name = data['property_name']
    color = data['color']
    default_image = data['default_image']
    filenames = data['filenames']
    
    my_namespace = uuid.NAMESPACE_DNS  

    my_uuid = uuid.uuid5(my_namespace, path + node_id)
    
    filename = os.path.basename(path)

    print('filename : ',  filenames, filename)
    
    current_utc_time = datetime.utcnow()
    gmt_plus_4_time = current_utc_time.replace(tzinfo=pytz.utc).astimezone(gmt_plus_4)

    gmt_plus_4_time_str = gmt_plus_4_time.strftime("%Y-%m-%d %H:%M:%S")
    
    if path in filenames:
        filenames.remove(path)
        return {'message' : f"Document already exists in database.",  "URL" : path}
        

#         main_prompt = f"""Get neo4j schema with relationships from current text - '{content}' """
#         main_prompt.replace('Resume', '')
    
#         while 1:
#             try:
#                 completion = openai.ChatCompletion.create(
#                     model=model_engine,
#                     messages=[
#                         {"role": "user", "content": main_prompt.strip()}],
#                     temperature = 0.1 ** 100
#                 )

#                 schema = (completion["choices"][0]["message"]["content"]).replace("Neo4j schema:", "").strip()
            
#                 print(schema)
            
#                 nodes = preprocess.Preprocess(schema).nodes
#                 edges = preprocess.Preprocess(schema).edges
            
#                 # return json.dumps({'file_name' : filename, 'nodes' : nodes, 'edges' : edges})

    try: 
        if not file:
            req = {
            "doc_id" : str(my_uuid),
            "path" : path,
            "project_id" : project_id,
            "user_id" : user_id,
            "node_id" : node_id,
            "type_id" : type_id,
            "property_id" : property_id,
            "node_name" : node_name,
            "type_name" : type_name,
            "property_name" : property_name,
            "color" : color,
            "default_image" : default_image,
            "filename" : name,
            "page" : 0,
            "page_content": "",
            "created": str(gmt_plus_4_time_str),
            }
            

            es.index(index='my_index', id = str(my_uuid), document=req)
            raise Exception
            
        if filename.endswith('.pdf'): texts = await asyncio.wait_for(extract_text_from_pdf(pdf_file=file), upload_timeout - request_time)

        elif filename.endswith('.docx') or filename.endswith('.doc') or filename.endswith('.msword') or filename.endswith('.document'): texts = await asyncio.wait_for(extract_text_from_doc(doc_file=file), upload_timeout - request_time)

        elif filename.endswith('.pptx') or filename.endswith('.ppt'): texts = await asyncio.wait_for(extract_text_from_ppt(ppt_file=file), upload_timeout - request_time)
        
        else: return {'message' : f"Invalid type of document", "URL" : path}
            
    except asyncio.TimeoutError:
        req = {
            "doc_id" : str(my_uuid),
            "path" : path,
            "project_id" : project_id,
            "user_id" : user_id,
            "node_id" : node_id,
            "type_id" : type_id,
            "property_id" : property_id,
            "node_name" : node_name,
            "type_name" : type_name,
            "property_name" : property_name,
            "color" : color,
            "default_image" : default_image,
            "filename" : name,
            "page" : 0,
            "page_content": "",
            "created": str(gmt_plus_4_time_str),
        }
        

        es.index(index='my_index', id = str(my_uuid), document=req)
        
        return {'message' : f"Document reading timeout", "URL" : path}
    
    except Exception as e:
        req = {
            "doc_id" : str(my_uuid),
            "path" : path,
            "project_id" : project_id,
            "user_id" : user_id,
            "node_id" : node_id,
            "type_id" : type_id,
            "property_id" : property_id,
            "node_name" : node_name,
            "type_name" : type_name,
            "property_name" : property_name,
            "color" : color,
            "default_image" : default_image,
            "filename" : name,
            "page" : 0,
            "page_content": "",
            "created": str(gmt_plus_4_time_str),
        }
        

        es.index(index='my_index', id = str(my_uuid), document=req)
        return {'message' : "Failed to read document", "URL" : path}
        
    finally:
        if file:
            file.close()
    
    for page_num, page_content in enumerate(texts):
        
        current_utc_time = datetime.utcnow()
        gmt_plus_4_time = current_utc_time.replace(tzinfo=pytz.utc).astimezone(gmt_plus_4)

        gmt_plus_4_time_str = gmt_plus_4_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Print the page number and text content to the console
        req = {
            "doc_id" : str(my_uuid),
            "path" : path,
            "project_id" : project_id,
            "user_id" : user_id,
            "node_id" : node_id,
            "type_id" : type_id,
            "property_id" : property_id,
            "node_name" : node_name,
            "type_name" : type_name,
            "property_name" : property_name,
            "color" : color,
            "default_image" : default_image,
            "filename" : name,
            "page" : page_num + 1,
            "page_content": page_content,
            "created": str(gmt_plus_4_time_str),
        }
        

        es.index(index='my_index', id = str(my_uuid) + str(page_num), document=req)
        
    
    return {'message' : f"Document was created in database", "URL" : path}
    
    
@app.route("/delete_node", methods=["DELETE"])
async def delete_node():
    
    try:
        project_id = request.json['project_id']
        node_id = request.json['node_id']
    except:
        abort(403, "Invalid raw data")
        
    all_docs = await get_list()
    file_ids = list(set([(item['doc_id'], item['path']) for item in all_docs.json['docs'] if (item['node_id'] == node_id) and (item['project_id'] == project_id)]))
    
    if not file_ids:
        return {'message' : f"There's no document with {project_id} project_id and {node_id} node_id"}
    
    
    else: 
        delete_ids = [delete(doc_id[0], doc_id[1]) for doc_id in file_ids]
        return {"messages" : await asyncio.gather(*delete_ids)}
    
    
@app.route("/search", methods=["POST"])
def get_page():

    try:
        keyword = request.json['search']
        page = request.json['page']
        project_id = request.json['project_id']
        list_type_id = request.json['list_type_id']
        limit = request.json['limit']
        sortOrder = request.json['sortOrder']
        sortField = request.json['sortField']
        
    except: abort(403, "Invalid raw data")
    
    if len(keyword.strip()) < 3:
        abort(422, "Search terms must contain at least 3 characters")
    
    scroll_size = limit  # Number of documents to retrieve in each scroll request
    scroll_timeout = "1m"  # Time interval to keep the search context alive
    special_characters = ['\\', '+', '-', '=', '&&', '||', '>', '<', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', "/"]
    
    for character in special_characters:
        
        keyword = keyword.replace(character, '\\' + character)
        # print(keyword)
        
    query1 = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "page_content": {
                                "query": keyword.strip(),
                                "operator" : "AND",
                                "fuzziness": "AUTO",
                                "analyzer" : "my_analyzer",
                            }
                        }
                    },
                    {
                        "query_string": {
                            "query": "*" + keyword.strip() + "*",
                            "analyzer" : "my_analyzer",
                        }
                    },
                    {
                        "match": {
                            "filename": {
                                "query": keyword.strip(),
                                "operator" : "AND",
                                "fuzziness": "AUTO",
                                "analyzer" : "my_analyzer",
                            }
                        }
                    },
                ]
            }
        },
        "highlight": {
            "fields": {
                "page_content": {
                    "type": "plain",
                    "fragmenter": "span",
                    "number_of_fragments" : 1000,
                    "order": "score"
                    },
                "filename" : {
                    "type": "plain",
                    "number_of_fragments" : 0,
                    "fragmenter": "span"
                    }
            }
        },
        "_source": ["path", "page", "project_id", "node_id", "user_id", "type_id", "property_id", "type_name", "property_name", "node_name", "filename", "color", "default_image", "created"]
    }
    
    query2 = {
            "query":{  
                "bool":{  
                    "must":[  
                        {  
                            "span_near":{  
                                "clauses":[],
                                "in_order":"true"
                            }
                        }
                    ]
                }
            },
                        
            "highlight": {
                "fields": {
                    "page_content": 
                        {   
                            "type": "plain",
                            "fragmenter": "span",
                            "number_of_fragments" : 1000,
                            "order": "score" 
                        },
                    "filename" : {
                            "type": "plain",
                            "number_of_fragments" : 0,
                            "fragmenter": "span"}
                            }
                    },
                "_source": ["path", "page", "project_id", "node_id", "user_id", "type_id", "property_id", "type_name", "property_name", "node_name", "filename", "color", "default_image", "created"]

            }
    
    if not re.match(r'.*[ +].*', keyword.strip()):
        try:
            result = es.search(index='my_index', body=query1, scroll=scroll_timeout, size=scroll_size)
            # print(result)

        except ConnectionError : abort(504, "Elasticsearch : Connection Timeout error")
        
        except:
            abort(504, "Elasticsearch : Search Timeout error")
        
        hits = result["hits"]["hits"]
        # print(hits)
        
    else: 
        for splited_text in keyword.strip().split():
                print(splited_text.strip())
                query2["query"]["bool"]["must"][0]["span_near"]["clauses"].append({  
                    "span_multi":{  
                        "match":{  
                            "fuzzy":{  
                                "page_content":{  
                                    "value":splited_text.strip(),
                                    "fuzziness": 2
                                }
                            },

                        }
                    }
                })
       
        # search for documents in the index and get only the ids
        try:
            result = es.search(index='my_index', body=query2, scroll=scroll_timeout, size=scroll_size)

        except ConnectionError : abort(504, "Elasticsearch : Connection Timeout error")

        except Exception as e:
            abort(500, str(e))
            
        hits = result["hits"]["hits"]
        
        if not hits:
            try:
                query2['query']['bool']['must'][0]['span_near']['in_order'] = 'false'
                result = es.search(index='my_index', body=query2, scroll=scroll_timeout, size=scroll_size)
            except ConnectionError : abort(504, "Elasticsearch : Connection Timeout error")

            except:
                abort(504, "Elasticsearch : Search Timeout error")
            
            hits = result["hits"]["hits"]
            
            if not hits:
                try:
                    keyword = keyword.replace(' ', '')
                    query1["query"]["bool"]["should"][0]['match']['page_content']['query'] = keyword
                    query1["query"]["bool"]["should"][1]['query_string']['query'] = '*' + keyword + '*'
                    query1["query"]["bool"]["should"][2]['match']['filename']['query'] = keyword
                    
                    result = es.search(index='my_index', body=query1, scroll=scroll_timeout, size=scroll_size)
                    hits = result["hits"]["hits"]
                    # print(hits)
                except ConnectionError : abort(504, "Elasticsearch : Connection Timeout error")
                except:
                    abort(504, "Elasticsearch : Search Timeout error")

    
    sentences = {}
    while hits:
        # Scroll to the next batch of results
        for hit in hits:
            # print(hit['highlight'].get('page_content'))
            if 'highlight' in hit.keys() and hit["_source"]["project_id"] == project_id and (hit["_source"]["type_id"] in list_type_id or not list_type_id):
                # print(hit['_score'], hit['highlight'].get('page_content', ['']))
                if (hit["_source"]["path"], hit["_source"]["node_id"]) not in sentences.keys():
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])] = defaultdict(int)
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["match_count"] = 0
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["match_filename"] = hit['highlight'].get('filename', [''])[0]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["node_id"] = hit["_source"]["node_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["project_id"] = hit["_source"]["project_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["user_id"] = hit["_source"]["user_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["type_id"] = hit["_source"]["type_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["node_id"] = hit["_source"]["node_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["property_id"] = hit["_source"]["property_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["type_name"] = hit["_source"]["type_name"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["node_name"] = hit["_source"]["node_name"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["property_name"] = hit["_source"]["property_name"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["filename"] = hit["_source"]["filename"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["default_image"] = hit["_source"]["default_image"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["color"] = hit["_source"]["color"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["created"] = hit["_source"]["created"]
                if hit['highlight'].get('page_content', [''])[0] and "match_content" not in sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]:
                    print(hit['highlight'].get('page_content', [''])[0])
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["match_content"] =  hit['highlight'].get('page_content', [''])[0].strip()
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["page"] = hit["_source"]["page"]
                for content in hit['highlight'].get('page_content', []):
                    # print("content - ", content)
                    print(re.findall(r"<em>(.*?)</em>", content))
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])]["match_count"] += int(len(re.findall(r"<em>(.*?)</em>", content)))
                    
                    
        scroll_id = result.get("_scroll_id")
        try:
            result = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        except:
            abort(504, "Elasticsearch : Search Timeout error")
            
        hits = result["hits"]["hits"]


    rows = []
    print(sentences.keys())
    for url, item in sentences.items():
        new_dict = defaultdict()
        item['path'] = url[0]
        keys = ('node_id', 'node_name', 'project_id', 'property_id', 'property_name', 'type_id', 'type_name', 'color', 'default_image')
        
        for key in keys:
            new_dict[key] = item[key]
            del item[key]
        # print(dict(item))
        if new_dict['node_id'] not in [row['node_id'] for row in rows]: 
            new_dict['updated'] = item['created']
            new_dict['data'] = [item]
            rows.append(new_dict)
            
        else:
            for i, data in enumerate(rows):
                if data['node_id'] == new_dict['node_id']:
                    break 
            updated = max(item['created'], rows[i]['updated'])
            rows[i]['updated'] = updated
            rows[i]['data'].append(item)
        
    if sortOrder == 'DESC' and sortField == 'name':
        rows.sort(key=lambda x: x['type_name'], reverse=True)
    elif sortOrder == 'DESC' and sortField == 'updated_at':
        rows.sort(key=lambda x: x['updated'], reverse=True)
    elif sortOrder == 'ASC' and sortField == 'name':
        rows.sort(key=lambda x: x['type_name'])
    elif sortOrder == 'ASC' and sortField == 'updated_at':
        rows.sort(key=lambda x: x['updated'])
    else: abort(403, 'Invalid sortOrder and/or sortField value')
            
    return jsonify({'rows' : rows[limit * (page-1) : limit * page], 'count' : len(rows), 'status' : 200})
    
    
    
@app.route("/get_list", methods=["GET"])
async def get_list():

    query = {"query": {"match_all": {}}, "size": 1}

    # Use the initial search API to retrieve the first batch of documents and the scroll ID
    try:
        initial_search = es.search(index='my_index', body=query, scroll='1m')
    except Exception as e:
        return {"message" : str(e)}
    scroll_id = initial_search['_scroll_id']
    total_results = initial_search['hits']['total']['value']

    # Iterate through the batches of results using the Scroll API
    documents = []
    while total_results > 0:
        for hit in initial_search['hits']['hits']:
            document = {
                "filename": hit["_source"]["filename"],
                "doc_id": hit["_source"]["doc_id"],
                "type_id": hit["_source"]["type_id"],
                "page": hit["_source"]["page"],
                "page_content" : hit["_source"]["page_content"],
                "created": hit["_source"]["created"],
                "project_id": hit["_source"]["project_id"],
                "node_id": hit["_source"]["node_id"],
                "path": hit["_source"]["path"]
            }
            documents.append(document)

        # Perform the next scroll request
        initial_search = es.scroll(scroll_id=scroll_id, scroll='1s')
        scroll_id = initial_search['_scroll_id']
        total_results -= len(initial_search['hits']['hits'])
        if len(initial_search['hits']['hits']) == 0:
            break

    # Clear the scroll context when done
    es.clear_scroll(scroll_id=scroll_id)
        # Print the list of documents
    
    return jsonify({'docs' : documents, 'status' : 200})
    
    
# @app.route("/delete/<string:document_id>", methods=["DELETE"])
async def delete(document_id, path):
    
    query = {
        "query": {
            "match": {
                "doc_id": document_id
            }
        }
    }

    # Use the delete_by_query API to delete all documents that match the query
    response = es.delete_by_query(index='my_index', body=query)
    print(response)
    if response['deleted']:
        return {'message' : "Document was deleted from database.", 'URL' : path}
    else: return {'message' : "Document doesn't exist in database.", 'URL' : path}
    
    
@app.route("/clean", methods=["DELETE"])    
async def clean():
    query = {
        "query": {
            "match_all": {}
        }
    }

    if es.delete_by_query(index='my_index', body=query)['deleted']:
        return jsonify({'message' : f"Elasticsearch database has cleaned successfully.", 'status' : 200})
    else: return jsonify({'message' : f"No document found in Elasticsearch database.", 'status' : 200})
