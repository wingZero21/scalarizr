BEGIN TRANSACTION;
DELETE FROM sqlite_sequence;
CREATE TABLE p2p_message (
    "id" INTEGER PRIMARY KEY,
    "message_id" TEXT,
    "response_id" TEXT,
    "message_name" TEXT,
    "message" TEXT,
    "queue" TEXT,
    "is_ingoing" INTEGER,
    "out_is_delivered" INTEGER,
    "out_delivery_attempts" INTEGER,
    "out_last_attempt_time" TEXT,
    "in_is_handled" INTEGER
);
INSERT INTO "p2p_message" VALUES(68,'b78ebcc4-cc17-4d3b-8e1a-81c05499bd0b',NULL,'HostUp','<?xml version="1.0" ?>
<message id="b78ebcc4-cc17-4d3b-8e1a-81c05499bd0b" name="HostUp"><meta><item name="a">b</item></meta><body><item name="cx">dd</item></body></message>','control',1,NULL,NULL,NULL,1);
INSERT INTO "p2p_message" VALUES(69,'f3478ccd-4f64-4b1f-81ea-7b28b1f43fd7',NULL,'HostUp','<?xml version="1.0" ?>
<message id="f3478ccd-4f64-4b1f-81ea-7b28b1f43fd7" name="HostUp"><meta><item name="a">b</item></meta><body><item name="cx">dd</item></body></message>','control',0,1,NULL,'2009-12-14 15:57:26',NULL);
INSERT INTO "p2p_message" VALUES(70,'f3478ccd-4f64-4b1f-81ea-7b28b1f43fd7',NULL,'HostUp','<?xml version="1.0" ?>
<message id="f3478ccd-4f64-4b1f-81ea-7b28b1f43fd7" name="HostUp"><meta><item name="a">b</item></meta><body><item name="cx">dd</item></body></message>','control',1,NULL,NULL,NULL,1);
COMMIT;
