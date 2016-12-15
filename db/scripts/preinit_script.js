
// Create collections
db.createCollection('cameo_countries')
db.createCollection('cameo_ethnic')
db.createCollection('cameo_eventcodes')
db.createCollection('cameo_events')
db.createCollection('cameo_mentions')
db.createCollection('cameo_goldsteinscale')
db.createCollection('cameo_knowngroup')
db.createCollection('cameo_religion')
db.createCollection('cameo_type')
db.createCollection('fips_country')
db.createCollection('coll_overall_stats')
db.createCollection('coll_metadata')
db.createCollection('coll_mentions_timeline')
db.createCollection('coll_linked_locations')
db.createCollection('coll_impact_map')
db.createCollection('coll_high_impact_regions')
db.createCollection('coll_high_impact_events')
db.createCollection('coll_globe_viz')
db.createCollection('coll_file_urls')
db.createCollection('coll_event_timeline')
db.createCollection('coll_articles_per_category')
db.createCollection('coll_actor_network')

// Create indexes for fast retrieval and avoiding timeouts + avoiding exceeding buffer size errors

// Indexes for the CAMEO events collections
db.cameo_events.createIndex({DATEADDED : 1})
db.cameo_events.createIndex({DATEADDED : -1})

db.cameo_events.createIndex({DATEADDED : 1, GLOBALEVENTID : 1})
db.cameo_events.createIndex({DATEADDED : 1, GLOBALEVENTID : -1})
db.cameo_events.createIndex({DATEADDED : -1, GLOBALEVENTID : -1})
db.cameo_events.createIndex({DATEADDED : -1, GLOBALEVENTID : 1})

db.cameo_events.createIndex({DATEADDED : 1, Actor1Name : 1})
db.cameo_events.createIndex({DATEADDED : 1, Actor1Name : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor1Name : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor1Name : 1})

db.cameo_events.createIndex({DATEADDED : 1, Actor1Geo_FullName : 1})
db.cameo_events.createIndex({DATEADDED : 1, Actor1Geo_FullName : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor1Geo_FullName : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor1Geo_FullName : 1})

db.cameo_events.createIndex({DATEADDED : 1, Actor2Name : 1})
db.cameo_events.createIndex({DATEADDED : 1, Actor2Name : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor2Name : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor2Name : 1})

db.cameo_events.createIndex({DATEADDED : 1, Actor2Geo_FullName : 1})
db.cameo_events.createIndex({DATEADDED : 1, Actor2Geo_FullName : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor2Geo_FullName : -1})
db.cameo_events.createIndex({DATEADDED : -1, Actor2Geo_FullName : 1})

db.cameo_events.createIndex({DATEADDED : 1, GoldsteinScale : 1})
db.cameo_events.createIndex({DATEADDED : 1, GoldsteinScale : -1})
db.cameo_events.createIndex({DATEADDED : -1, GoldsteinScale : -1})
db.cameo_events.createIndex({DATEADDED : -1, GoldsteinScale : 1})

db.cameo_events.createIndex({DATEADDED : 1, NumMentions : 1})
db.cameo_events.createIndex({DATEADDED : 1, NumMentions : -1})
db.cameo_events.createIndex({DATEADDED : -1, NumMentions : -1})
db.cameo_events.createIndex({DATEADDED : -1, NumMentions : 1})

db.cameo_events.createIndex({DATEADDED : 1, AvgTone : 1})
db.cameo_events.createIndex({DATEADDED : 1, AvgTone : -1})
db.cameo_events.createIndex({DATEADDED : -1, AvgTone : -1})
db.cameo_events.createIndex({DATEADDED : -1, AvgTone : 1})

db.cameo_events.createIndex({AvgTone : 1})
db.cameo_events.createIndex({AvgTone : -1})

db.cameo_events.createIndex({GoldsteinScale : -1})
db.cameo_events.createIndex({GoldsteinScale : 1})


// Indexes for the CAMEO mentions collections

db.cameo_mentions.createIndex({MentionTimeDate : 1})
db.cameo_mentions.createIndex({MentionTimeDate : -1})


db.cameo_mentions.createIndex({MentionTimeDate : 1, Confidence : 1})
db.cameo_mentions.createIndex({MentionTimeDate : 1, Confidence : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, Confidence : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, Confidence : 1})

db.cameo_mentions.createIndex({MentionTimeDate : 1, MentionDocTone : 1})
db.cameo_mentions.createIndex({MentionTimeDate : 1, MentionDocTone : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, MentionDocTone : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, MentionDocTone : 1})

db.cameo_mentions.createIndex({MentionTimeDate : 1, MentionSourceName : 1})
db.cameo_mentions.createIndex({MentionTimeDate : 1, MentionSourceName : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, MentionSourceName : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, MentionSourceName : 1})

db.cameo_mentions.createIndex({MentionTimeDate : 1, MentionType : 1})
db.cameo_mentions.createIndex({MentionTimeDate : 1, MentionType : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, MentionType : -1})
db.cameo_mentions.createIndex({MentionTimeDate : -1, MentionType : 1})


db.cameo_mentions.createIndex({Confidence : 1})
db.cameo_mentions.createIndex({Confidence : -1})

db.cameo_mentions.createIndex({MentionDocTone : -1})
db.cameo_mentions.createIndex({MentionDocTone : 1})



