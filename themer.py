#!/usr/bin/python3
import os
import rdflib

import helpers

def build_themes_query():
    """ Create a SPARQL-query for fetching all curated themes """
    return """
        PREFIX ost: <http://w3id.org/ost/ns#> #Open Standard for Tourism Ecosystems Data
        SELECT DISTINCT ?theme WHERE {{
            GRAPH <{0}> {{
                ?event <{1}> ?theme.
            }}
        }}
        """.format(os.getenv('MU_APPLICATION_GRAPH'),
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasCuratedTheme")

def build_topicscore_query(theme):
    """ Create a SPARQL-query for fetching all topics and their corresponding
    scores (weights) from all events that have a certain curated theme
    """
    return """
        PREFIX ost: <http://w3id.org/ost/ns#> #Open Standard for Tourism Ecosystems Data
        SELECT DISTINCT ?topic ?topicscore WHERE {{
            GRAPH <{0}> {{
                ?event <{1}> <{2}>;
                    ost:infoUrl/<{3}> ?score.
                ?score <{4}> ?topicscore;
                    <{5}> ?topic.
            }}
        }}
        """.format(os.getenv('MU_APPLICATION_GRAPH'),
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasCuratedTheme",
                   theme,
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopicScore",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasScore",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopic")

def build_topicprint_query(theme, topicscores):
    """ Create a SPARQL-query for inserting the total (weighted) scores for
    topics related to a certain curated theme.
    topicscores takes a dictionary with {topic: score} -pairs
    """
    mu_uri = "http://mu.semte.ch/vocabularies/ext/topic-tools/"
    voc_ns = rdflib.namespace.Namespace(mu_uri + "voc/")
    # res_ns = rdflib.namespace.Namespace(mu_uri + "resources/")
    graph = rdflib.graph.Graph()

    for topic, score in topicscores.items():
        topic_uri = rdflib.term.URIRef(mu_uri + "resources/" + "TopicPrint/" +
                                       helpers.generate_uuid())

        graph.add((rdflib.term.URIRef(theme), # Topic id
                   voc_ns["hasTopicPrint"],
                   topic_uri))
        graph.add((topic_uri, # Topic id
                   rdflib.namespace.RDF["type"],
                   voc_ns["TopicPrint"]))
        graph.add((topic_uri, # Topic string
                   voc_ns["hasTopic"],
                   rdflib.term.URIRef(topic)))
        graph.add((topic_uri, # Topic string
                   voc_ns["hasTopicScore"],
                   rdflib.term.Literal(score)))

    return """INSERT DATA {{
            GRAPH <{0}> {{
                {1}
            }}
        }}
    """.format(os.getenv('MU_APPLICATION_GRAPH'),
               graph.serialize(format='nt').decode('utf-8'))

# Query for topic scores per theme/event
def evaluate_theme(theme):
    themes_query = build_topicscore_query(theme)
    try:
        topics = helpers.query(themes_query)["results"]["bindings"]
        helpers.log(topics)
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed:\n" + str(e))
    topichash = {}
    for topic in topics:
        topicscore = float(topic["topicscore"]["value"])
        topicuri = topic["topic"]["value"]
        try:
            topichash[topicuri] += topicscore
        except KeyError:
            topichash[topicuri] = topicscore

    weightedhash = {k: v/sum(topichash.values()) for k, v in topichash.items()} #weighing (divide by number of events assigned to a category)

    helpers.update(build_topicprint_query(theme, weightedhash))

def run():
    try:
        results = helpers.query(build_themes_query())["results"]["bindings"]
        helpers.log(str(results))
        themes = [theme["theme"]["value"] for theme in results]
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed:\n" + str(e))

    for theme in themes:
        try:
            evaluate_theme(theme)
        except Exception as e:
            helpers.log("theme failed"+str(e))
            continue
