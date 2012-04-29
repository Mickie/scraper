exports.index = function(req, res){
	
  var pg = require('pg');
  var theDBUrl = process.env.DATABASE_URL || "tcp://fanzo_site:fanzo_site@localhost/fanzo_site_development";

  pg.connect( theDBUrl, function(err, client) {
    if (err)
    {
       console.log(err);
       res.render('index', { title: 'Express' + JSON.stringify(err) });
       return;
    }

    var query = client.query('SELECT * FROM sports');

    query.on('row', function(row) {
      res.render('index', { title: 'Express' + JSON.stringify(row) });
    });
  });

};



