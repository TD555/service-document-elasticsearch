from flask import Flask, jsonify, request, json, abort, make_response
from elasticsearch import Elasticsearch, ConnectionError, BadRequestError, exceptions
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import ConflictError
import xml.etree.ElementTree as ET
import xmltodict
import uuid
import asyncio
import aiohttp
from datetime import datetime
import requests
from collections import defaultdict
from random import random
import tempfile
import uuid
import traceback
import time
import pytz
import openpyxl
import xlrd
import subprocess

import fitz
import os
import re
import io

from version import __version__, __description__

app = Flask(__name__)

# ES_INDEX = "araks_index"
# AMAZON_URL = "https://araks-projects-develop.s3.amazonaws.com/"
# ES_HOST = "http://localhost:9201/"


ES_INDEX = os.environ['ELASTICSEARCH_INDEX']
AMAZON_URL = os.environ['AMAZON_URL']
ES_HOST = os.environ['ELASTICSEARCH_URL']


es = Elasticsearch([ES_HOST])

request_timeout = 30
upload_timeout = 30

gmt_plus_4 = pytz.timezone("Asia/Dubai")


put_data = {
    "settings": {
        "analysis": {
            "analyzer": {
                "my_analyzer": {"tokenizer": "standard", "filter": ["lowercase"]}
            }
        }
    },
    "mappings": {
        "properties": {
            "created": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
            "page_content": {"type": "text", "analyzer": "my_analyzer"},
        }
    },
}


try:
    es.indices.create(index=ES_INDEX, body=put_data)

except BadRequestError as e:
    # print(str(e))
    pass


settings = {"highlight.max_analyzed_offset": 10000000}

try:
    es.indices.put_settings(index=ES_INDEX, settings=settings)
    OFFSET = 10000000
except BadRequestError as e:
    # print(str(e))
    OFFSET = 1000000
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


async def extract_text_from_pdf(pdf_file):

    all_texts = []
    pdf_document = fitz.open("pdf", pdf_file.read())

    # Iterate through each page
    for page_num in range(pdf_document.page_count):
        page = pdf_document.load_page(page_num)

        # Extract text from the page
        text = page.get_text("text")

        text = text.replace("\n", " ")
        clean_text = re.sub(r'\s+', ' ', text)

        all_texts.append(clean_text)

    # Close the PDF document
    pdf_document.close()

    return all_texts


def create_doc(es, **kwargs):
    es.index(index=ES_INDEX, id=kwargs["doc_id"] +
             str(kwargs["page"]), document=kwargs)


async def extract_text_from_doc(doc_file):
    try:
        doc_file.seek(0)

        document_content = io.BytesIO(doc_file.read())
        temp_file = tempfile.NamedTemporaryFile(suffix=".docx")

        temp_file.write(document_content.getvalue())
        document_path = temp_file.name

        pdf_bytes = subprocess.check_output(
            ["unoconv", "-f", "pdf", "--stdout", document_path]
        )

        # Create a BytesIO object from the PDF content
        pdf_stream = io.BytesIO(pdf_bytes)

        all_texts = await extract_text_from_pdf(pdf_stream)
        temp_file.close()

        return all_texts

    except Exception as e:
        return {"message": e.stderr}


async def extract_text_from_ppt(ppt_file):
    try:
        ppt_file.seek(0)

        ppt_content = io.BytesIO(ppt_file.read())
        temp_file = tempfile.NamedTemporaryFile(suffix=".pptx")
        temp_file.write(ppt_content.getvalue())
        ppt_path = temp_file.name

        pdf_bytes = subprocess.check_output(
            ["unoconv", "-f", "pdf", "--stdout", ppt_path]
        )
        pdf_stream = io.BytesIO(pdf_bytes)

        all_texts = await extract_text_from_pdf(pdf_stream)

        temp_file.close()

        return all_texts

    except Exception as e:
        return {"message": e.stderr}


async def extract_text_from_xlsx(xlsx_file):
    try:
        temp_buffer = io.BytesIO(xlsx_file.read())

        workbook = openpyxl.load_workbook(temp_buffer)

        all_texts = []

        for sheet_name in workbook.sheetnames:
            sheet = workbook[sheet_name]

            row_str = []
            # Loop through each row in the sheet
            for row in sheet.iter_rows(values_only=True):
                # print(row)
                row_str.append(" ".join(map(str, row)))

                # Print the formatted row
            all_texts.append(sheet_name + " " + " ".join(row_str).strip())

        workbook.close()
        temp_buffer.close()

        return all_texts

    except Exception as e:
        return {"message": e.stderr}


async def extract_text_from_xls(xls_file):
    try:
        temp_buffer = io.BytesIO(xls_file.read())

        xls_workbook = xlrd.open_workbook(file_contents=temp_buffer.read())

        all_texts = []

        for sheet_name in xls_workbook.sheet_names():
            sheet = xls_workbook.sheet_by_name(sheet_name)

            rows = []

            # Loop through each row in the sheet
            for row_num in range(sheet.nrows):
                row = sheet.row_values(row_num)
                rows.append(" ".join(map(str, row)))

            all_texts.append(sheet_name + " " + " ".join(rows).strip())

        return all_texts

    except Exception as e:
        return {"message": e.stderr}


@app.errorhandler(Exception)
def handle_error(error):
    # Get the traceback
    error_traceback = traceback.format_exc()
    print(error_traceback)
    if hasattr(error, "code"):
        status_code = error.code
    else:
        status_code = 500
    return {"message": str(error).strip(), "status_code": status_code}, status_code


@app.after_request
def after_request(response):
    response.headers.set("Access-Control-Allow-Origin", "*")
    response.headers.set("Access-Control-Allow-Headers",
                         "Content-Type,Authorization")
    response.headers.set("Access-Control-Allow-Methods",
                         "GET,PUT,POST,DELETE,OPTIONS")
    return response


@app.route("/", methods=["GET"])
def info():
    return __description__


@app.route("/create_or_update", methods=["POST"])
async def create_or_update():
    #   ---Get file and parse content---
    data_dict = {}

    try:
        nodes_data = request.json["nodes_data"]
        data_dict["project_id"] = request.json["project_id"]
        data_dict["user_id"] = request.json["user_id"]
        data_dict["node_id"] = request.json["node_id"]
        data_dict["node_name"] = request.json["node_name"]
        data_dict["type_id"] = request.json["type_id"]
        data_dict["property_id"] = request.json["property_id"]
        data_dict["type_name"] = request.json["type"]
        data_dict["property_name"] = request.json["property"]
        data_dict["color"] = request.json["color"]
        data_dict["default_image"] = request.json["default_image"]

        # if (not data_dict['default_image'].startswith(AMAZON_URL)) and (data_dict['default_image']):
        #     data_dict['default_image'] = AMAZON_URL + data_dict['default_image']

    except:
        abort(422, "Invalid raw data")

    all_docs = await get_list(node_id=data_dict["node_id"], property_id=data_dict["property_id"])

    id_dict = {"doc_ids": defaultdict(list)}
    for item in all_docs.json["docs"]:
        id_dict["doc_ids"][item["path"]].append(
            item["doc_id"] + str(item["page"])
        )
        id_dict["source"] = {  # type: ignore
            "node_name": item["node_name"],
            "type_name": item["type_name"],
            "property_name": item["property_name"],
            "default_image": item["default_image"],
            "color": item["color"],
        }

    print("All filenames in the start", list(id_dict["doc_ids"].keys()))
    filenames = list(id_dict["doc_ids"].keys())
    data_dict["filenames"] = filenames
    data_dict["id_dict"] = id_dict

    returned_jsons = []

    for item in remove_duplicates(nodes_data):
        result = {**data_dict, **item}
        returned_jsons.append(upload_document(result))

    # print(returned_jsons)
    returned_jsons = await asyncio.gather(*returned_jsons)

    print("All filenames in the end", filenames)

    my_namespace = uuid.NAMESPACE_DNS

    for filename in filenames:
        my_uuid = uuid.uuid5(my_namespace, filename + data_dict["node_id"])
        old_id = str(my_uuid)
        delete_response = await delete(old_id, filename)
        returned_jsons.append(delete_response)

    # print((await get_list()).json['docs'])
    return jsonify({"messages": returned_jsons, "status": 200})


async def upload_document(data):
    # parsed = parser.from_buffer(file.read())
    # text = parsed["content"]
    # content = text.strip()

    try:
        name = data["name"]
        path = data["url"]

        if not path.startswith(AMAZON_URL):
            path = AMAZON_URL + path

    except:
        return {"message": f"Invalid key names in nodes_data"}

    try:
        start = time.time()
        file = await asyncio.wait_for(
            get_filestorage_object(path), timeout=request_timeout
        )
        end = time.time()
        request_time = end - start

    except Exception:
        return {"message": f"Document reading timeout", "URL": path}

    project_id = data["project_id"]
    user_id = data["user_id"]
    node_id = data["node_id"]
    node_name = data["node_name"]
    type_id = data["type_id"]
    property_id = data["property_id"]
    type_name = data["type_name"]
    property_name = data["property_name"]
    color = data["color"]
    default_image = data["default_image"]
    filenames = data["filenames"]
    id_dict = data["id_dict"]

    my_namespace = uuid.NAMESPACE_DNS

    my_uuid = uuid.uuid5(my_namespace, path + node_id)

    filename = os.path.basename(path)

    current_utc_time = datetime.utcnow()
    gmt_plus_4_time = current_utc_time.replace(
        tzinfo=pytz.utc).astimezone(gmt_plus_4)

    gmt_plus_4_time_str = gmt_plus_4_time.strftime("%Y-%m-%d %H:%M:%S")

    if path in filenames:
        filenames.remove(path)
        update_request = {
            "doc": {
                "node_name": node_name,
                "type_name": type_name,
                "property_name": property_name,
                "default_image": default_image,
                "color": color,
            }
        }

        if id_dict["source"] != update_request["doc"]:
            # Update documents
            update_actions = []

            # Populate the list with update actions for each doc_id
            for doc_id in id_dict["doc_ids"][path]:
                update_actions.append(
                    {
                        "_op_type": "update",  # Specify the operation type
                        "_index": ES_INDEX,
                        "_id": doc_id,
                        "_source": update_request,  # Provide the update request for each document
                    }
                )

            # Use the bulk API to perform updates
            success, failed = bulk(es, update_actions)

            # Check for any failed updates
            if failed:
                for item in failed:
                    print(f"Failed to update document with ID {item['_id']}")

            else:
                return {"message": f"Document is updated.", "URL": path}
        return {"message": f"Document already exists in database.", "URL": path}

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
            create_doc(
                es,
                doc_id=str(my_uuid),
                path=path,
                project_id=project_id,
                user_id=user_id,
                node_id=node_id,
                type_id=type_id,
                property_id=property_id,
                node_name=node_name,
                type_name=type_name,
                property_name=property_name,
                color=color,
                default_image=default_image,
                filename=name,
                page=0,
                page_content="",
                created=str(gmt_plus_4_time_str),
            )
            raise Exception

        if filename.endswith(".pdf"):
            texts = await asyncio.wait_for(
                extract_text_from_pdf(
                    pdf_file=file), upload_timeout - request_time
            )

        elif (
            filename.endswith(".docx")
            or filename.endswith(".doc")
            or filename.endswith(".msword")
            or filename.endswith(".document")
        ):
            texts = await asyncio.wait_for(
                extract_text_from_doc(
                    doc_file=file), upload_timeout - request_time
            )

        elif filename.endswith(".pptx") or filename.endswith(".ppt"):
            texts = await asyncio.wait_for(
                extract_text_from_ppt(
                    ppt_file=file), upload_timeout - request_time
            )

        elif filename.endswith(".xlsx"):
            texts = await asyncio.wait_for(
                extract_text_from_xlsx(
                    xlsx_file=file), upload_timeout - request_time
            )

        elif filename.endswith(".xls"):
            texts = await asyncio.wait_for(
                extract_text_from_xls(
                    xls_file=file), upload_timeout - request_time
            )

        else:
            create_doc(
                es,
                doc_id=str(my_uuid),
                path=path,
                project_id=project_id,
                user_id=user_id,
                node_id=node_id,
                type_id=type_id,
                property_id=property_id,
                node_name=node_name,
                type_name=type_name,
                property_name=property_name,
                color=color,
                default_image=default_image,
                filename=name,
                page=0,
                page_content="",
                created=str(gmt_plus_4_time_str),
            )

            return {"message": f"Invalid type of document", "URL": path}

    except asyncio.TimeoutError:
        create_doc(
            es,
            doc_id=str(my_uuid),
            path=path,
            project_id=project_id,
            user_id=user_id,
            node_id=node_id,
            type_id=type_id,
            property_id=property_id,
            node_name=node_name,
            type_name=type_name,
            property_name=property_name,
            color=color,
            default_image=default_image,
            filename=name,
            page=0,
            page_content="",
            created=str(gmt_plus_4_time_str),
        )

        return {"message": f"Document reading timeout", "URL": path}

    except Exception as e:
        create_doc(
            es,
            doc_id=str(my_uuid),
            path=path,
            project_id=project_id,
            user_id=user_id,
            node_id=node_id,
            type_id=type_id,
            property_id=property_id,
            node_name=node_name,
            type_name=type_name,
            property_name=property_name,
            color=color,
            default_image=default_image,
            filename=name,
            page=0,
            page_content="",
            created=str(gmt_plus_4_time_str),
        )

        return {"message": "Failed to read document", "URL": path}

    finally:
        if file:
            file.close()

    if texts:
        for page_num, page_content in enumerate(texts):
            current_utc_time = datetime.utcnow()
            gmt_plus_4_time = current_utc_time.replace(tzinfo=pytz.utc).astimezone(
                gmt_plus_4
            )

            gmt_plus_4_time_str = gmt_plus_4_time.strftime("%Y-%m-%d %H:%M:%S")

            # Print the page number and text content to the console
            create_doc(
                es,
                doc_id=str(my_uuid),
                path=path,
                project_id=project_id,
                user_id=user_id,
                node_id=node_id,
                type_id=type_id,
                property_id=property_id,
                node_name=node_name,
                type_name=type_name,
                property_name=property_name,
                color=color,
                default_image=default_image,
                filename=name,
                page=page_num + 1,
                page_content=page_content,
                created=str(gmt_plus_4_time_str),
            )

    else:
        create_doc(
            es,
            doc_id=str(my_uuid),
            path=path,
            project_id=project_id,
            user_id=user_id,
            node_id=node_id,
            type_id=type_id,
            property_id=property_id,
            node_name=node_name,
            type_name=type_name,
            property_name=property_name,
            color=color,
            default_image=default_image,
            filename=name,
            page=0,
            page_content="",
            created=str(gmt_plus_4_time_str),
        )

    return {"message": f"Document was created in database", "URL": path}


@app.route("/delete_node", methods=["DELETE"])
async def delete_node():
    try:
        project_id = request.json["project_id"]
        node_id = request.json.get("node_id", None)
        property_id = request.json.get("property_id", None)

    except:
        abort(422, "Invalid raw data")

    all_docs = await get_list(project_id=project_id, node_id=node_id, property_id=property_id)

    file_ids = list(
        set(
            [
                (item["doc_id"], item["path"])
                for item in all_docs.json["docs"]
            ]
        )
    )

    if not file_ids:
        return {
            "message": f"No document exists to be deleted."
        }

    else:
        delete_ids = [delete(doc_id[0], doc_id[1]) for doc_id in file_ids]
        return {"messages": await asyncio.gather(*delete_ids)}


def initialize_queries(keyword):
    query1 = {
        "query": {
            "bool": {
                "should": [
                    {
                        "match": {
                            "page_content": {
                                "query": keyword.strip(),
                                "operator": "AND",
                                "fuzziness": "AUTO",
                                "analyzer": "my_analyzer",
                            }
                        }
                    },
                    {
                        "query_string": {
                            "query": "*" + keyword.strip() + "*",
                            "analyzer": "my_analyzer",
                        }
                    },
                    {
                        "match": {
                            "filename": {
                                "query": keyword.strip(),
                                "operator": "AND",
                                "fuzziness": "AUTO",
                                "analyzer": "my_analyzer",
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
                    "number_of_fragments": 1000,
                    "order": "score",
                    "max_analyzed_offset": OFFSET,
                },
                "filename": {
                    "type": "plain",
                    "number_of_fragments": 0,
                    "fragmenter": "span",
                },
            }
        },
        "_source": [
            "path",
            "page",
            "project_id",
            "node_id",
            "user_id",
            "type_id",
            "property_id",
            "type_name",
            "property_name",
            "node_name",
            "filename",
            "color",
            "default_image",
            "created",
        ],
    }

    query2 = {
        "query": {
            "bool": {"must": [{"span_near": {"clauses": [], "in_order": "true"}}]}
        },
        "highlight": {
            "fields": {
                "page_content": {
                    "type": "plain",
                    "fragmenter": "span",
                    "number_of_fragments": 1000,
                    "order": "score",
                    "max_analyzed_offset": OFFSET,
                },
                "filename": {
                    "type": "plain",
                    "number_of_fragments": 0,
                    "fragmenter": "span",
                },
            }
        },
        "_source": [
            "path",
            "page",
            "project_id",
            "node_id",
            "user_id",
            "type_id",
            "property_id",
            "type_name",
            "property_name",
            "node_name",
            "filename",
            "color",
            "default_image",
            "created",
        ],
    }

    return query1, query2


def sentence_search(keywords, query, method, scroll_timeout, scroll_size):
    for splited_text in keywords:

        if method == "regexp":
            query["query"]["bool"]["must"][0]["span_near"]["clauses"].append(
                {
                    "span_multi": {
                        "match": {
                            "regexp": {
                                "page_content": {
                                    "value": f".*{splited_text.strip().lower()}.*",
                                    "flags": "ALL"
                                }
                            },
                        }
                    }
                }
            )
        elif method == "fuzzy":
            query["query"]["bool"]["must"][0]["span_near"]["clauses"].append(
                {
                    "span_multi": {
                        "match": {
                            "fuzzy": {
                                "page_content": {
                                    "value": splited_text.strip().lower(),
                                    "fuzziness": "AUTO"
                                }
                            },
                        }
                    }
                }
            )
    try:
        result = es.search(
            index=ES_INDEX, body=query, scroll=scroll_timeout, size=scroll_size
        )

    except ConnectionError:
        abort(408, "Elasticsearch : Connection Timeout error")

    except Exception as e:
        abort(500, str(e))

    hits = result["hits"]["hits"]

    return result, hits


@app.route("/search", methods=["POST"])
def get_page():
    try:
        keyword = request.json["search"]
        page = request.json["page"]
        project_id = request.json["project_id"]
        list_type_id = request.json["list_type_id"]
        limit = request.json["limit"]
        sortOrder = request.json["sortOrder"]
        sortField = request.json["sortField"]

    except:
        abort(422, "Invalid raw data")

    if len(keyword.strip()) < 3:
        abort(422, "Search terms must contain at least 3 characters")

    scroll_size = limit  # Number of documents to retrieve in each scroll request
    scroll_timeout = "1m"  # Time interval to keep the search context alive
    special_characters = [
        "\\",
        "+",
        "-",
        "=",
        "&&",
        "||",
        ">",
        "<",
        "!",
        "(",
        ")",
        "{",
        "}",
        "[",
        "]",
        "^",
        '"',
        "~",
        "*",
        "?",
        ":",
        "/",
    ]

    for character in special_characters:
        keyword = keyword.replace(character, "\\" + character)

    keywords = keyword.strip().split()

    query1, query2 = initialize_queries(keyword)

    if not re.match(r".*[ +].*", keyword.strip()):
        try:
            result = es.search(
                index=ES_INDEX, body=query1, scroll=scroll_timeout, size=scroll_size
            )

        except ConnectionError:
            abort(408, "Elasticsearch : Connection Timeout error")

        except Exception as e:
            abort(500, str(e))

        hits = result["hits"]["hits"]

    else:
        result, hits = sentence_search(
            keywords, query2, "regexp", scroll_timeout, scroll_size)

        if not hits:
            try:
                query2["query"]["bool"]["must"][0]["span_near"]["in_order"] = "false"
                result = es.search(
                    index=ES_INDEX, body=query2, scroll=scroll_timeout, size=scroll_size
                )
            except ConnectionError:
                abort(408, "Elasticsearch : Connection Timeout error")

            except Exception as e:
                abort(500, str(e))

            hits = result["hits"]["hits"]

            if not hits:

                query1, query2 = initialize_queries(keyword)

                result, hits = sentence_search(
                    keywords, query2, "fuzzy", scroll_timeout, scroll_size)

                if not hits:
                    try:
                        query2["query"]["bool"]["must"][0]["span_near"]["in_order"] = "false"
                        result = es.search(
                            index=ES_INDEX, body=query2, scroll=scroll_timeout, size=scroll_size
                        )
                    except ConnectionError:
                        abort(408, "Elasticsearch : Connection Timeout error")

                    except Exception as e:
                        abort(500, str(e))

                    hits = result["hits"]["hits"]

                    if not hits:
                        try:
                            keyword = keyword.replace(" ", "")
                            query1["query"]["bool"]["should"][0]["match"]["page_content"][
                                "query"
                            ] = keyword
                            query1["query"]["bool"]["should"][1]["query_string"]["query"] = (
                                "*" + keyword + "*"
                            )
                            query1["query"]["bool"]["should"][2]["match"]["filename"][
                                "query"
                            ] = keyword

                            result = es.search(
                                index=ES_INDEX,
                                body=query1,
                                scroll=scroll_timeout,
                                size=scroll_size,
                            )
                            hits = result["hits"]["hits"]
                        except ConnectionError:
                            abort(408, "Elasticsearch : Connection Timeout error")

                        except Exception as e:
                            abort(500, str(e))

    sentences = {}
    while hits:
        # Scroll to the next batch of results
        for hit in hits:
            if (
                "highlight" in hit.keys()
                and hit["_source"]["project_id"] == project_id
                and (hit["_source"]["type_id"] in list_type_id or not list_type_id)
            ):
                if (
                    hit["_source"]["path"],
                    hit["_source"]["node_id"],
                ) not in sentences.keys():
                    sentences[
                        (hit["_source"]["path"], hit["_source"]["node_id"])
                    ] = defaultdict(int)
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "match_count"
                    ] = 0
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "match_filename"
                    ] = hit["highlight"].get("filename", [""])[0]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "node_id"
                    ] = hit["_source"]["node_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "project_id"
                    ] = hit["_source"]["project_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "user_id"
                    ] = hit["_source"]["user_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "type_id"
                    ] = hit["_source"]["type_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "node_id"
                    ] = hit["_source"]["node_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "property_id"
                    ] = hit["_source"]["property_id"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "type_name"
                    ] = hit["_source"]["type_name"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "node_name"
                    ] = hit["_source"]["node_name"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "property_name"
                    ] = hit["_source"]["property_name"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "filename"
                    ] = hit["_source"]["filename"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "default_image"
                    ] = hit["_source"]["default_image"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "color"
                    ] = hit["_source"]["color"]
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "created"
                    ] = hit["_source"]["created"]
                if (
                    hit["highlight"].get("page_content", [""])[0]
                    and "match_content"
                    not in sentences[
                        (hit["_source"]["path"], hit["_source"]["node_id"])
                    ]
                ):
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "match_content"
                    ] = (hit["highlight"].get("page_content", [""])[0].strip())
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "page"
                    ] = hit["_source"]["page"]
                for content in hit["highlight"].get("page_content", []):
                    # print(re.findall(r"<em>(.*?)</em>", content))
                    sentences[(hit["_source"]["path"], hit["_source"]["node_id"])][
                        "match_count"
                    ] += int(len(re.findall(r"<em>(.*?)</em>", content)))

        scroll_id = result.get("_scroll_id")
        try:
            result = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        except Exception as e:
            abort(500, str(e))

        hits = result["hits"]["hits"]

    rows = []
    for url, item in sentences.items():
        new_dict = defaultdict()
        item["path"] = url[0]
        keys = (
            "node_id",
            "node_name",
            "project_id",
            "property_id",
            "property_name",
            "type_id",
            "type_name",
            "color",
            "default_image",
        )

        for key in keys:
            new_dict[key] = item[key]
            del item[key]

        if new_dict["node_id"] not in [row["node_id"] for row in rows]:
            new_dict["updated"] = item["created"]
            new_dict["data"] = [item]
            rows.append(new_dict)

        else:
            for i, data in enumerate(rows):
                if data["node_id"] == new_dict["node_id"]:
                    break
            updated = max(item["created"], rows[i]["updated"])
            rows[i]["updated"] = updated
            rows[i]["data"].append(item)

    if sortOrder == "DESC" and sortField == "name":
        rows.sort(key=lambda x: x["type_name"], reverse=True)
    elif sortOrder == "DESC" and sortField == "updated_at":
        rows.sort(key=lambda x: x["updated"], reverse=True)
    elif sortOrder == "ASC" and sortField == "name":
        rows.sort(key=lambda x: x["type_name"])
    elif sortOrder == "ASC" and sortField == "updated_at":
        rows.sort(key=lambda x: x["updated"])
    else:
        abort(403, "Invalid sortOrder and/or sortField value")

    return jsonify(
        {
            "rows": rows[limit * (page - 1): limit * page],
            "count": len(rows),
            "status": 200,
        }
    )


@app.route("/get_list", methods=["GET"])
async def get_list(**search):

    if not search:
        query = {"query": {"match_all": {}}, "size": 10000}

    else:
        query = {
            "query": {
                "bool": {
                    "must": [
                    ]
                }
            }, "size": 10000}

        for key, value in search.items():
            if value:
                query['query']['bool']['must'].append(
                    {"term": {key + '.keyword': value}})

    # Use the initial search API to retrieve the first batch of documents and the scroll ID
    try:
        while True:
            if es.indices.exists(index=ES_INDEX):
                initial_search = es.search(
                    index=ES_INDEX, body=query, scroll="1m")
                break
            else:
                continue

    except Exception as e:
        abort(500, str(e))

    scroll_id = initial_search["_scroll_id"]
    total_results = initial_search["hits"]["total"]["value"]

    # Iterate through the batches of results using the Scroll API
    documents = []
    while total_results > 0:
        for hit in initial_search["hits"]["hits"]:
            document = {
                "filename": hit["_source"]["filename"],
                "doc_id": hit["_source"]["doc_id"],
                "type_id": hit["_source"]["type_id"],
                "type_name": hit["_source"]["type_name"],
                "property_id": hit["_source"]["property_id"],
                "property_name": hit["_source"]["property_name"],
                "page": hit["_source"]["page"],
                "page_content": hit["_source"]["page_content"],
                "created": hit["_source"]["created"],
                "project_id": hit["_source"]["project_id"],
                "node_id": hit["_source"]["node_id"],
                "node_name": hit["_source"]["node_name"],
                "default_image": hit["_source"]["default_image"],
                "color": hit["_source"]["color"],
                "path": hit["_source"]["path"],
            }
            documents.append(document)

        # Perform the next scroll request
        initial_search = es.scroll(scroll_id=scroll_id, scroll="1s")
        scroll_id = initial_search["_scroll_id"]
        total_results -= len(initial_search["hits"]["hits"])
        if len(initial_search["hits"]["hits"]) == 0:
            break

    # Clear the scroll context when done
    es.clear_scroll(scroll_id=scroll_id)
    # Print the list of documents

    return jsonify({"docs": documents, "status": 200})


# @app.route("/delete/<string:document_id>", methods=["DELETE"])
async def delete(document_id, path):
    query = {"query": {"term": {"doc_id.keyword": document_id}}}

    # Use the delete_by_query API to delete all documents that match the query
    try:
        response = es.delete_by_query(
            index=ES_INDEX, body=query, scroll_size=10000)

    except Exception as e:
        abort(409, str(e))

    if response["deleted"]:
        return {"message": "Document was deleted from database.", "URL": path}
    else:
        return {"message": "Document doesn't exist in database.", "URL": path}


@app.route("/clean", methods=["DELETE"])
async def clean():
    query = {"query": {"match_all": {}}}

    if es.delete_by_query(index=ES_INDEX, body=query, scroll_size=10000)["deleted"]:
        return jsonify(
            {
                "message": f"Elasticsearch database has cleaned successfully.",
                "status": 200,
            }
        )
    else:
        return jsonify(
            {"message": f"No document found in Elasticsearch database.", "status": 200}
        )


def sort_dict(my_dict: dict):
    return {
        k: v
        for k, v in sorted(my_dict.items(), key=lambda x: (-x[1], x[0]), reverse=False)
    }


def get_count(nodes: list, source_target: list = None) -> dict:
    if not source_target:
        source_target = nodes
    return {node: source_target.count(node) for node in set(nodes)}


suggestions = [
    {
        "text": "There are {node} types of nodes and {edge} types of relations in the following graph scheme. Below are the types of nodes and connections:"
    },
    {
        "text": "Graph scheme with {node} types of nodes and {edge} types of relations. All node types and all connections are listed below:"
    },
    {
        "text": "The following graph scheme presents {node} types of nodes and {edge} types of relations. Here are all the node types and all their connections:"
    },
    {
        "text": "There are {node} types of nodes in the graph and {edge} kinds of connections. Here are everything you need to know about nodes and connections:"
    },
    {
        "text": "This graph contains {node} types of nodes and {edge} types of relationships. Here are the types of nodes and connections:"
    },
    {
        "text": "In the following graph scheme, there are {node} types of nodes and {edge} types of relations. Here are the types of nodes and all connections:"
    },
    {
        "text": "A graph scheme with {node} types of nodes and {edge} types of relations is shown below. The types of nodes and the types of connections are as follows:"
    },
    {
        "text": "This graph scheme contains {node} types of nodes and {edge} types of relationships. Here are the types of nodes and all connections:"
    },
    {
        "text": "This graph scheme presents {node} types of nodes and {edge} types of relations. Here are all the types of nodes and all the connections:"
    },
    {
        "text": "Graph scheme showing {node} types of nodes and {edge} types of relations. Here are the types of nodes and their connections:"
    },
    {
        "text": "This graph scheme has {node} types of nodes and {edge} types of relations. These are the types of nodes and the types of connections:"
    },
    {
        "text": "{edge} types of relations and {node} types of nodes are shown in the following graph scheme. The types of nodes and connections are as follows:"
    },
]


@app.route("/scheme_to_text", methods=["POST"])
async def get_scheme():
    # call to API and get data

    scheme_data = request.json["data"]

    if not scheme_data:
        input_sentence = "The following graph schema does not yet contain anything."

    relations = sort_dict(
        get_count(
            [
                data["source"] + " -> " +
                data["name"] + " -> " + data["target"]
                for data in scheme_data["edges"]
            ]
        )
    )
    scheme = [
        {
            "source": data["source"],
            "relation": data["name"],
            "target": data["target"],
        }
        for data in scheme_data["edges"]
    ]
    most_nodes = sort_dict(
        get_count(
            [item["source"] for item in scheme] + [item["target"]
                                                   for item in scheme]
        )
    )
    input_sentence = suggestions[int(random() * len(suggestions))]["text"].format(
        node=len(most_nodes), edge=len(set([item["relation"] for item in scheme]))
    )

    str_nodes = ""
    str_rels = ""
    for i, node in enumerate(most_nodes):
        str_nodes += str(i + 1) + "." + node + "\n"

    for j, rel in enumerate(relations):
        str_rels += str(j + 1) + "." + rel + "\n"

    input_sentence += f"\nNodes:\n{str_nodes}"
    input_sentence += f"\nRelations:\n{str_rels}"
    return jsonify({"text": input_sentence, "status": 200})


namespace = uuid.NAMESPACE_DNS

SEARCH_URL = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=pubmed&term={keyword}&retmode=json&retmax={limit}&retstart={offset}'
FETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={id}&rettype=medline&retmode=xml"


def convert_date(pubDate):
    months = {
        "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04",
        "May": "05", "Jun": "06", "Jul": "07", "Aug": "08",
        "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"
    }
    day = pubDate.get("Day", '01')
    month = months.get(pubDate.get("Month", '01'), '01')
    year = pubDate["Year"]
    return f"{year}-{month}-{day}"


def convert_to_text(item, abstract=False):
    if isinstance(item, dict):
        return item.get("#text", "")
    if abstract and isinstance(item, list):
        text = '\n\n'.join([element["@Label"] + '\n' + element["#text"] for element in item])
        return text  
    return item


async def fetch_with_retry(session, id, retry_attempts=10):
    for attempt in range(retry_attempts):
        try:
            async with session.get(FETCH_URL.format(id=id.strip())) as response:
                response.raise_for_status()
                xml_file = await response.text()
                xml_data = ET.fromstring(xml_file)
                xmlstr = ET.tostring(xml_data, encoding='utf-8', method='xml')
                dict_data = dict(xmltodict.parse(xmlstr))[
                    'PubmedArticleSet']['PubmedArticle']['MedlineCitation']
                
                title = convert_to_text(dict_data['Article']['ArticleTitle'])
                id_data = {
                    'article': {
                        'id': id,
                        'name': title[:50],
                        'article_url': '',
                        'source': 'PubMed',
                        'title': title,
                        'abstract': convert_to_text(dict_data['Article'].get('Abstract',{'AbstractText' : ''})["AbstractText"], True),
                        'pub_date': convert_date(dict_data['Article']['Journal']['JournalIssue']['PubDate']),
                        'language': dict_data['Article'].get('Language', '')},
                    'country': dict_data['MedlineJournalInfo'].get('Country', ''),
                }

                elocation = dict_data['Article'].get('ELocationID')
                if isinstance(elocation, dict) and elocation.get('@EIdType') == 'doi':
                    id_data['article']['article_url'] = 'https://www.doi.org/' + \
                        elocation.get('#text', '')
                elif isinstance(elocation, list):
                    for eid in elocation:
                        if eid.get('@EIdType') == 'doi':
                            id_data['article']['article_url'] = 'https://www.doi.org/' + \
                                eid.get('#text', '')
                                
                authors = dict_data['Article'].get('AuthorList', {'Author' : []})['Author']
                if isinstance(authors, list):
                    authors = authors[:20]
                else: authors = [authors]
                id_data['authors'] = [
                    {
                        "affiliation": author.get('AffiliationInfo', {"Affiliation": ""})["Affiliation"],
                        'name': (author.get('ForeName', '') + ' ' + author.get('LastName', '')).strip()[:50],
                        'id': uuid.uuid5(namespace, (author.get('ForeName', '') + ' ' + author.get('LastName', '') + ' ' + author.get('AffiliationInfo', {"Affiliation": ""})["Affiliation"]).strip())
                    }
                    if isinstance(author.get('AffiliationInfo'), dict)
                    else
                    {
                        "affiliation": ' '.join([item["Affiliation"] for item in author.get('AffiliationInfo', [{"Affiliation": ""}])]),
                        'name': (author.get('ForeName', '') + ' ' + author.get('LastName', '')).strip()[:50],
                        'id': uuid.uuid5(namespace, (author.get('ForeName', '') + ' ' + author.get('LastName', '') + ' ' + ' '.join([item["Affiliation"] for item in author.get('AffiliationInfo', [{"Affiliation": ""}])])).strip())
                    }
                    for author in authors if (author.get('ForeName', '') + ' ' + author.get('LastName', '')).strip()
                ]
                
                keywords = dict_data.get('KeywordList', {}).get('Keyword', [])
                if not isinstance(keywords, list):
                    keywords = [keywords]
                id_data['keywords'] = [keyword.get(
                    "#text", "") for keyword in keywords] if keywords else []

                return id_data

        except aiohttp.ClientError as ce:
            if attempt < retry_attempts - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            else:
                abort(500, f"Failed to fetch details for ID {id}: {ce}")
        except Exception as e:
            print(f"Error processing ID {id}: {e}")
            break
            abort(500, f"Error processing ID {id}: {e}")


@app.route('/pubmed/get_data', methods=["POST"])
async def pubmed_preview():
    try:
        keyword = str(request.json['keyword'])
        limit = request.json.get('limit', 5)
        page = int(request.json.get('page', 1))
    except Exception as e:
        abort(422, "Invalid raw data. One of the parameters is incorrect. {str(e)}")
        
    if len(keyword) < 3:
            abort(400, "Keyword must be at least 3 characters long")
            
    async with aiohttp.ClientSession() as session:
        response = await session.get(SEARCH_URL.format(keyword=keyword, limit=limit, offset=(page-1)*limit))
        search_result = (await response.json())['esearchresult']
        id_list = search_result['idlist']
        await asyncio.sleep(0.1)
        tasks = [fetch_with_retry(session, id) for id in id_list]
        all_data = await asyncio.gather(*tasks)

    all_data = [data for data in all_data if data]

    return jsonify({'count': search_result['count'], 'articles': all_data})
