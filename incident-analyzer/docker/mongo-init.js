// runs once on first container start
// creates the incidents db, collections, and indexes

db = db.getSiblingDB("incidents");

db.createCollection("incidents");
db.createCollection("events");
db.createCollection("alerts");

// incidents: query by status, severity, service
db.incidents.createIndex({ status: 1 });
db.incidents.createIndex({ severity: -1 });
db.incidents.createIndex({ service: 1 });
db.incidents.createIndex({ started_at: -1 });

// events: mostly written, occasionally queried by service/time window
db.events.createIndex({ service: 1, timestamp: -1 });
db.events.createIndex({ timestamp: -1 });

// alerts: query by source and time
db.alerts.createIndex({ source: 1, fired_at: -1 });

print("MongoDB initialized — collections and indexes created.");
