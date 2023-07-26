from flask import Flask, jsonify, request, json, abort, render_template
from elasticsearch import Elasticsearch, ConnectionTimeout, BadRequestError, exceptions
import asyncio
from datetime import datetime
import requests
from collections import defaultdict
from docx2pdf import convert
import tempfile
import uuid
import traceback
import math
import time
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

URL = "http://192.168.0.176:5000"
INDEX = 'my_index'

es_host = os.environ['ELASTICSEARCH_URL']
# es_host = "http://localhost:9200/"
es = Elasticsearch([es_host]) 

request_timeout = 20
upload_timeout = 40

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
          "analyzer" : "my_analyzer"
      }
    }
  }
}


try:
    es.indices.create(index="my_index", body=put_data)
    
except BadRequestError: 
    pass


async def get_filestorage_object(url):
    response = await asyncio.get_event_loop().run_in_executor(None, requests.get, url)
    if response.status_code == 200:
        file_object = io.BytesIO(response.content)
        return file_object
    else:
        # Handle error cases
        return None


def get_context(output_stream, interpreter, all_texts, all_pages):
    for page in all_pages:

        interpreter.process_page(page)

        text = output_stream.getvalue()

        # do something with the text for this page
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
    

async def extract_text_from_doc(doc_file):

    doc_file.seek(0)
    
    document_content = io.BytesIO(doc_file.read())
    temp_file = tempfile.NamedTemporaryFile(suffix='.docx', delete=False)
    temp_file.write(document_content.getvalue())
    temp_file.close()
    document_path = temp_file.name
   

    generated_id = str(uuid.uuid1())

    out_file = os.path.abspath(f'{generated_id}.pdf')

    convert(document_path, out_file)
    
    
    with open(f'{generated_id}.pdf', 'rb') as pdf_file:
        all_texts = await extract_text_from_pdf(pdf_file)
    
    os.remove(f'{generated_id}.pdf')
    os.remove(document_path)

    return all_texts
    

@app.errorhandler(Exception)
def handle_error(error):
    # Get the traceback
    error_traceback = traceback.format_exc()
    if hasattr(error, 'code'):
        status_code = error.code
    else:
        status_code = 500
    print(error_traceback)
    return {"message": error.description, "status" : status_code}
    
    
@app.after_request
def after_request(response):
  response.headers.set('Access-Control-Allow-Origin', '*')
  response.headers.set('Access-Control-Allow-Headers', 'Content-Type,Authorization')
  response.headers.set('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
  return response 


@app.route("/", methods=["GET"])
def info():
    return __description__


@app.route("/upload", methods=["POST"])
async def upload():

#   ---Get file and parse content---

    data_dict = {}
    
    try:
        data_dict['path'] = request.form['path']
        data_dict['project_id'] = request.form['project_id']
        data_dict['user_id'] = request.form['user_id']
        data_dict['node_id'] = request.form['node_id']
        data_dict['node_name'] = request.form['node_name']
        data_dict['type_id'] = request.form['type_id']
        data_dict['property_id'] = request.form['property_id']
        data_dict['type_name'] = request.form['type']
        data_dict['property_name'] = request.form['property']
        data_dict['color'] = request.form['color']
        data_dict['default_image'] = request.form['default_image']
    except:
        abort(403, "Invalid form-data")
        
    returned_json = await upload_document(data_dict)
    if returned_json['status'] == 403:
        abort(403, returned_json['message'])
    return jsonify(returned_json)
    
    
async def upload_document(data):    
    # parsed = parser.from_buffer(file.read())
    # text = parsed["content"]
    # content = text.strip()

    path = data['path']
    try:
        start = time.time()
        file = await asyncio.wait_for(get_filestorage_object(path), timeout=request_timeout)
        end = time.time()
        request_time = end - start
        
    except Exception: 
        abort(408, 'Document reading timeout.')
        
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
    
    my_namespace = uuid.NAMESPACE_DNS  

    my_uuid = uuid.uuid5(my_namespace, path)
    
    items = (await get_list()).json['docs']
    
    filename = os.path.basename(path)

    for item in items:
        if (item['doc_id'] == str(my_uuid)):
            return {'message' : f"Document already exists in database.",  "name" : filename, 'status' : 403}
        

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
            raise Exception
        if filename.endswith('.pdf'):
            texts = await asyncio.wait_for(extract_text_from_pdf(pdf_file=file), upload_timeout - request_time)
         
        else: texts = await asyncio.wait_for(extract_text_from_doc(doc_file=file), upload_timeout - request_time)
        
        print(texts)
    
    except asyncio.TimeoutError:
        abort(408, "Document reading timeout")
    except Exception:
        abort(500, 'Failed to read document.')   
        
    finally:
        file.close()
    
    for page_num, page_content in enumerate(texts):

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
            "filename" : filename,
            "page" : page_num + 1,
            "page_content": page_content,
            "created": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        }
        

        es.index(index=INDEX, id = str(my_uuid) + str(page_num), document=req)
        
    
    return {'message' : f"Document was created in database", "doc_id" : str(my_uuid), "name" : filename, "created" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'status' : 200}
    
    

@app.route("/search", methods=["POST"])
def get_page():

    keyword = request.json['search']
    page = request.json['page']
    project_id = request.json['project_id']
    list_type_id = request.json['list_type_id']
    limit = request.json['limit']
    
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
                    "fragmenter": "span"},
                "filename" : {
                    "type": "plain",
                    "fragmenter": "span"}
            }
        },
        "_source": ["path", "page", "project_id", "node_id", "user_id", "type_id", "property_id", "type_name", "property_name", "node_name", "color", "default_image"]
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
                            "order": "score" 
                        },
                    }
                },
                "_source": ["path", "page", "project_id", "node_id", "user_id", "type_id", "property_id", "type_name", "property_name", "node_name", "color", "default_image"]
            }
    
    if not re.match(r'.*[ +].*', keyword.strip()):
        try:
            result = es.search(index="my_index", body=query1, scroll=scroll_timeout, size=scroll_size)
            # print(result)

        except ConnectionTimeout: abort(504, "Elasticsearch : Connection Timeout error")
        
        except:
            abort(504, "Elasticsearch :Search Timeout error")
        
        hits = result["hits"]["hits"]
        # print(hits)
        
    else: 
        for splited_text in keyword.strip().split():
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
            result = es.search(index="my_index", body=query2, scroll=scroll_timeout, size=scroll_size)

        except ConnectionTimeout: abort(504, "Elasticsearch : Connection Timeout error")

        except:
            abort(504, "Elasticsearch : Search Timeout error")
            
        hits = result["hits"]["hits"]
        
        if not hits:
            try:
                query2['query']['bool']['must'][0]['span_near']['in_order'] = 'false'
                result = es.search(index="my_index", body=query2, scroll=scroll_timeout, size=scroll_size)
            except ConnectionTimeout: abort(504, "Elasticsearch : Connection Timeout error")

            except:
                abort(504, "Elasticsearch : Search Timeout error")
            
            hits = result["hits"]["hits"]
            
            if not hits:
                try:
                    keyword = keyword.replace(' ', '')
                    query1["query"]["bool"]["should"][0]['match']['page_content']['query'] = keyword
                    query1["query"]["bool"]["should"][1]['query_string']['query'] = '*' + keyword + '*'
                    query1["query"]["bool"]["should"][2]['match']['filename']['query'] = keyword
                    
                    result = es.search(index="my_index", body=query1, scroll=scroll_timeout, size=scroll_size)
                    hits = result["hits"]["hits"]
                    print(hits)
                except ConnectionTimeout: abort(504, "Elasticsearch : Connection Timeout error")
                except:
                    abort(504, "Elasticsearch : Search Timeout error")

    
    sentences = {}
    while hits:
        # Scroll to the next batch of results
        for hit in hits:
            if 'highlight' in hit.keys() and hit["_source"]["project_id"] == project_id and (hit["_source"]["type_id"] in list_type_id or not list_type_id):
                # print(hit['_score'], hit['highlight'].get('page_content', ['']))
                if hit["_source"]["path"] not in sentences.keys():
                    sentences[hit["_source"]["path"]] = defaultdict(int)
                    sentences[hit["_source"]["path"]]["match_count"] = 0
                    sentences[hit["_source"]["path"]]["match_filename"] = hit['highlight'].get('filename', [''])[0]
                    sentences[hit["_source"]["path"]]["node_id"] = hit["_source"]["node_id"]
                    sentences[hit["_source"]["path"]]["project_id"] = hit["_source"]["project_id"]
                    sentences[hit["_source"]["path"]]["user_id"] = hit["_source"]["user_id"]
                    sentences[hit["_source"]["path"]]["type_id"] = hit["_source"]["type_id"]
                    sentences[hit["_source"]["path"]]["node_id"] = hit["_source"]["node_id"]
                    sentences[hit["_source"]["path"]]["property_id"] = hit["_source"]["property_id"]
                    sentences[hit["_source"]["path"]]["type_name"] = hit["_source"]["type_name"]
                    sentences[hit["_source"]["path"]]["node_name"] = hit["_source"]["node_name"]
                    sentences[hit["_source"]["path"]]["property_name"] = hit["_source"]["property_name"]
                    sentences[hit["_source"]["path"]]["page"] = hit["_source"]["page"]
                    sentences[hit["_source"]["path"]]["default_image"] = hit["_source"]["default_image"]
                    sentences[hit["_source"]["path"]]["color"] = hit["_source"]["color"]
                    sentences[hit["_source"]["path"]]["match_content"] =  hit['highlight'].get('page_content', [''])[0]
                    
                for content in hit['highlight'].get('page_content', []):
                    sentences[hit["_source"]["path"]]["match_count"] += int(len(re.findall(r"<em>(.*?)</em>", content)))
                    
                    
        scroll_id = result.get("_scroll_id")
        try:
            result = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        except:
            abort(504, "Elasticsearch : Search Timeout error")
            
        hits = result["hits"]["hits"]


    rows = []
    
    for url, item in sentences.items():
        new_dict = defaultdict()
        item['path'] = url
        keys = ('node_id', 'project_id', 'property_id', 'property_name', 'type_id', 'type_name', 'color', 'default_image')
        
        for key in keys:
            new_dict[key] = item[key]
            del item[key]

        
        if new_dict['node_id'] not in [row['node_id'] for row in rows]: 
            new_dict['data'] = [item]
            rows.append(new_dict)
            
        else:
            for i, data in enumerate(rows):
                if data['node_id'] == new_dict['node_id']:
                    break 
            rows[i]['data'].append(item)
            
    return jsonify({'rows' : rows[limit * (page-1) : limit * page], 'count' : math.ceil(len(rows) / limit), 'status' : 200})
    
    
    
@app.route("/get_list", methods=["GET"])
async def get_list():

    # Define the fields you want to retrieve
    fields = ["doc_id", "page", "project_id", "node_id", "user_id", "type_id", "property_id", "type_name", "property_name", "filename", "created", "path"]

    # Define the search query to retrieve all documents
    query = {"size": 10000, "query": {"match_all": {}}}

    # Use the search API to retrieve the documents and extract the fields
    results = es.search(index=INDEX, body=query, _source=fields)

    # Iterate through the results and extract the fields from each document
    documents = []
    for hit in results["hits"]["hits"]:
        document = {"name": hit["_source"]["filename"], "doc_id": hit["_source"]["doc_id"], "type_id" : hit["_source"]["type_id"], "page" : hit["_source"]["page"],\
                    "created": hit["_source"]["created"], "project_id" : hit["_source"]["project_id"], "node_id" : hit["_source"]["node_id"], "path" : hit["_source"]["path"]}
        documents.append(document)
        
    # Print the list of documents
    
    return jsonify({'docs' : documents, 'status' : 200})
    

@app.route("/update", methods=["PUT"])
async def update():

    my_namespace = uuid.NAMESPACE_DNS
    my_uuid = uuid.uuid5(my_namespace, request.form["old_path"])
    
    old_id = str(my_uuid)
    
    data_dict = {}
    
    
    if request.form['old_path'] == request.form['path']:
        abort(400, "Old and new files are duplicated.")
    
    data_dict['path'] = request.form["path"]
    data_dict['project_id'] = request.form['project_id']
    data_dict['user_id'] = request.form['user_id']
    data_dict['node_id'] = request.form['node_id']
    data_dict['type_id'] = request.form['type_id']
    data_dict['property_id'] = request.form['property_id']
    data_dict['node_name'] = request.form['node_name']
    data_dict['type_name'] = request.form['type']
    data_dict['property_name'] = request.form['property']
    data_dict['color'] = request.form['type']
    data_dict['default_image'] = request.form['default_image']
    
    
    delete_response = await delete(old_id)
    
    await asyncio.sleep(0.5)
    
    upload_response = await upload_document(data_dict)
    
    returned_dict = {"delete_message" : delete_response["message"], "upload_message" : upload_response["message"]}
    
    if not delete_response['status'] == 200 and not upload_response['status'] == 200:
        returned_dict['status'] = 400
        return returned_dict
    else:
        returned_dict["status"] = 200
    
    return jsonify(returned_dict)

    
    
@app.route("/delete/<string:document_id>", methods=["DELETE"])
async def delete(document_id):
    
    query = {
        "query": {
            "match": {
                "doc_id": document_id
            }
        }
    }

    # Use the delete_by_query API to delete all documents that match the query
    response = es.delete_by_query(index=INDEX, body=query)
    print(response)
    if response['deleted']:
        return {'message' : f"Document was deleted from database.", 'status' : 200}
    else: return {'message' : f"Document doesn't exist in database.", 'status' : 400}
    
    
@app.route("/clean", methods=["DELETE"])    
async def clean():
    query = {
        "query": {
            "match_all": {}
        }
    }

    if es.delete_by_query(index=INDEX, body=query)['deleted']:
        return jsonify({'message' : f"Elasticsearch database has cleaned successfully.", 'status' : 200})
    else: return jsonify({'message' : f"No document found in Elasticsearch database.", 'status' : 200})
