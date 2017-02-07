#!/usr/bin/python3
import os
import rdflib
import traceback

import helpers

def build_themes_query():
    """ Create a SPARQL-query for fetching all curated themes """
    return """
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
        SELECT DISTINCT ?topic ?score WHERE {{
            GRAPH <{0}> {{
                ?event <{1}> <{2}>;
                    ost:infoUrl/<{3}> ?topicscore.
                ?topicscore <{4}> ?score;
                            <{5}> ?topic.
            }}
        }}
        """.format(os.getenv('MU_APPLICATION_GRAPH'),
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasCuratedTheme",
                   theme,
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopicScore",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasScore",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopic")

def build_events_query():
    """ Create a SPARQL-query for fetching all topics and their corresponding
    scores (weights) from all events
    """
    return """
        PREFIX ost: <http://w3id.org/ost/ns#> #Open Standard for Tourism Ecosystems Data
        SELECT DISTINCT ?event ?topic ?score WHERE {{
            GRAPH <{0}> {{
                ?event ost:infoUrl/<{1}> ?topicscore.
                ?topicscore <{2}> ?topic;
                            <{3}> ?score.
            }}
        }}
        """.format(os.getenv('MU_APPLICATION_GRAPH'),
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopicScore",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopic",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasScore")

def build_topicprint_select_query():
    """ Create a SPARQL-query for fetching all curated themes and their
    topicprints.
    """
    return """
        SELECT DISTINCT ?theme ?topic ?score WHERE {{
            GRAPH <{0}> {{
                ?event <{1}> ?theme.
                ?theme <{2}> ?topicprint.
                ?topicprint <{3}> ?topic;
                            <{4}> ?score.
            }}
        }}
        """.format(os.getenv('MU_APPLICATION_GRAPH'),
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasCuratedTheme",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopicPrint",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasTopic",
                   "http://mu.semte.ch/vocabularies/ext/topic-tools/voc/hasScore")

def build_topicprint_update_query(theme, topicscores):
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
        graph.add((topic_uri,
                   rdflib.namespace.RDF["type"],
                   voc_ns["TopicPrint"]))
        graph.add((topic_uri,
                   voc_ns["hasTopic"],
                   rdflib.term.URIRef(topic)))
        graph.add((topic_uri,
                   voc_ns["hasScore"],
                   rdflib.term.Literal(score)))

    return """INSERT DATA {{
            GRAPH <{0}> {{
                {1}
            }}
        }}
    """.format(os.getenv('MU_APPLICATION_GRAPH'),
               graph.serialize(format='nt').decode('utf-8'))

def build_learnedthemes_update_query(event, learnedthemes):
    """ Create a SPARQL-query for attaching the machine-learned themes and their
    weights to a certain event
    learnedthemes takes a dictionary with {theme: score} -pairs
    """
    mu_uri = "http://mu.semte.ch/vocabularies/ext/topic-tools/"
    voc_ns = rdflib.namespace.Namespace(mu_uri + "voc/")
    # res_ns = rdflib.namespace.Namespace(mu_uri + "resources/")
    graph = rdflib.graph.Graph()

    for theme, score in learnedthemes.items():
        themescore_uri = rdflib.term.URIRef(mu_uri + "resources/" +
                                            "LearnedThemeScore/" +
                                            helpers.generate_uuid())

        graph.add((rdflib.term.URIRef(event),
                   voc_ns["hasLearnedThemeScore"],
                   themescore_uri))
        graph.add((themescore_uri,
                   rdflib.namespace.RDF["type"],
                   voc_ns["LearnedThemeScore"]))
        graph.add((themescore_uri,
                   voc_ns["hasLearnedTheme"],
                   rdflib.term.URIRef(theme)))
        graph.add((themescore_uri,
                   voc_ns["hasScore"],
                   rdflib.term.Literal(score)))

    return """INSERT DATA {{
            GRAPH <{0}> {{
                {1}
            }}
        }}
    """.format(os.getenv('MU_APPLICATION_GRAPH'),
               graph.serialize(format='nt').decode('utf-8'))

def evaluate_theme(theme):
    """Calculate & insert topic-fingerprint for a given theme"""
    themes_query = build_topicscore_query(theme)
    try:
        topicscores = helpers.query(themes_query)["results"]["bindings"]
        #helpers.log(topicscores)
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed:\n" + str(e))
        return
    topichash = {}
    for topicscore in topicscores:
        score = float(topicscore["score"]["value"])
        topicuri = topicscore["topic"]["value"]
        try:
            topichash[topicuri] += score
        except KeyError:
            topichash[topicuri] = score

    #weighing: divide by sum of all scores (so that sum of all topicprints = 1)
    weightedhash = {k: v/sum(topichash.values()) for k, v in topichash.items()}

    try:
        helpers.update(build_topicprint_update_query(theme, weightedhash))
        helpers.log('Calculated & inserted topic-fingerprint for theme "{}" ({} topicprints)'.format(theme, len(topichash)))
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed, exiting:\n" + str(e))
        return

def multiply_dicts(x, y):
    return {k: x.get(k, 0) * y.get(k, 0) for k in set(x) & set(y)}

def run():
    # fetch each curated theme, calculate & insert topic-fingerprint
    try:
        results = helpers.query(build_themes_query())["results"]["bindings"]
        themes = [theme["theme"]["value"] for theme in results]
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed, exiting:\n" + str(e))
        return

    for theme in themes:
        try:
            evaluate_theme(theme)
        except Exception as e:
            helpers.log("theme failed"+str(e))
            continue

    # Build a dictionary of topicscores ("topicprints") per curated theme
    try:
        results = helpers.query(build_topicprint_select_query())["results"]["bindings"]
        helpers.log('Queried topicprints for all curated themes ({} topicprints)'.format(len(results)))
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed, exiting:\n" + str(e))
        return
    curatedthemes = {}
    for result in results:
        theme = result["theme"]["value"]
        topic = result["topic"]["value"]
        score = float(result["score"]["value"])
        try:
            curatedthemes[theme][topic] = score
        except KeyError:
            curatedthemes[theme] = {topic: score}

    # Build a dictionary of topicscores per event
    try:
        results = helpers.query(build_events_query())["results"]["bindings"]
        helpers.log('Queried topicscores for all curated themes ({} topicscores)'.format(len(results)))
    except Exception as e:
        helpers.log("Querying SPARQL-endpoint failed, exiting:\n" + str(e))
        return
    events = {}
    for result in results:
        event = result["event"]["value"]
        topic = result["topic"]["value"]
        score = float(result["score"]["value"])
        try:
            events[event][topic] = score
        except KeyError:
            events[event] = {topic: score}

    # Make a map by event of it's topicscores and weights by theme (topicprints) multiplied
    weights_by_event_by_cat = {}
    for event, topicscores_event in events.items():
        d = {theme: sum(multiply_dicts(topicscores_event, topicscores_theme).values()) \
            for theme, topicscores_theme in curatedthemes.items()}
        weights_by_event_by_cat[event] = d

    # Add learned themes to graph, event by event
    # TODO do this in previous loop without building dict for all events (less stack)
    for event, themes in weights_by_event_by_cat.items():
        try:
            helpers.update(build_learnedthemes_update_query(event, themes))
            helpers.log('Learned & inserted themes for event "{}" ({} themes)'.format(event, len(themes)))
        except Exception as e:
            helpers.log("Querying SPARQL-endpoint failed:\n" + str(e))
