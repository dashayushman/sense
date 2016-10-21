var express = require('express');
var path = require('path');
var favicon = require('serve-favicon');
var logger = require('morgan');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');

var routes = require('./routes/index');
var themeSimple = require('./routes/themeSimple');
var indexDark = require('./routes/indexDark');
var sample = require('./routes/sample');
var getActorNetwork = require('./routes/getActorNetwork');
var getMentionsTimeline = require('./routes/getMentionsTimeline');
var getArticlesPerCategory = require('./routes/getArticlesPerCategory');
var getHighImpactEvents = require('./routes/getHighImpactEvents');
var getHighImpactRegions = require('./routes/getHighImpactRegions');
var getLinkedLocations = require('./routes/getLinkedLocations');
var getOverallStats = require('./routes/getOverallStats');
var getMetadata = require('./routes/getMetadata');
var getGlobalImpactMapData = require('./routes/getGlobalImpactMap');

var app = express();

// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'jade');

// uncomment after placing your favicon in /public
app.use(favicon(path.join(__dirname, 'public', 'madm_fav_ico.png')));
app.use(logger('dev'));
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser.json({limit: '100mb'}));
app.use(bodyParser.raw({limit: '100mb'}));
app.use(bodyParser.text({limit: '100mb'}));

app.use('/', routes);
app.use('/themeSimple', themeSimple);
app.use('/indexDark', indexDark);
app.use('/sample', sample);
app.use('/getActorNetwork', getActorNetwork);
app.use('/getMentionsTimeline', getMentionsTimeline);
app.use('/getArticlesPerCategory', getArticlesPerCategory);
app.use('/getHighImpactEvents', getHighImpactEvents);
app.use('/getHighImpactRegions', getHighImpactRegions);
app.use('/getLinkedLocations', getLinkedLocations);
app.use('/getOverallStats', getOverallStats);
app.use('/getMetadata', getMetadata);
app.use('/getGlobalImpactMapData', getGlobalImpactMapData);

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
  app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
      message: err.message,
      error: err
    });
  });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
  res.status(err.status || 500);
  res.render('error', {
    message: err.message,
    error: {}
  });
});

app.listen(4000, function () {
  console.log('Example app listening on port 4000!');
});


module.exports = app;
