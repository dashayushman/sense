# CAMEO Countries

mongoimport -d db_sense -c cameo_countries --type tsv --file "../../GDELT2.0/codebooks/CAMEO.country.txt" --headerline

# CAMEO.ethnic

mongoimport -d db_sense -c cameo_ethnic --type tsv --file "../../GDELT2.0/codebooks/CAMEO.ethnic.txt" --headerline

# CAMEO.eventcodes

mongoimport -d db_sense -c cameo_eventcodes --type tsv --file "../../GDELT2.0/codebooks/CAMEO.eventcodes.txt" --headerline

# CAMEO.goldsteinscale

mongoimport -d db_sense -c cameo_goldsteinscale --type tsv --file "../../GDELT2.0/codebooks/CAMEO.goldsteinscale.txt" --headerline

# CAMEO.knowngroup


mongoimport -d db_sense -c cameo_knowngroup --type tsv --file "../../GDELT2.0/codebooks/CAMEO.knowngroup.txt" --headerline

# CAMEO.religion

mongoimport -d db_sense -c cameo_religion --type tsv --file "../../GDELT2.0/codebooks/CAMEO.religion.txt" --headerline

# CAMEO.type

mongoimport -d db_sense -c cameo_type --type tsv --file "../../GDELT2.0/codebooks/CAMEO.type.txt" --headerline

# FIPS.country

mongoimport -d db_sense -c fips_country --type tsv --file "../../GDELT2.0/codebooks/FIPS.country.txt" --headerline

# GDELT.mentions.type

mongoimport -d db_sense -c gdelt_mentions_type --type tsv --file "../../GDELT2.0/codebooks/GDELT.mentions.sources.txt" --headerline
