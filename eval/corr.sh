sqlite=/usr/local/Cellar/sqlite/*/bin/sqlite3
$sqlite answers.db .dump > dump
$sqlite test_drb.db < dump
