-- Describe P2P_MESSAGE
CREATE TABLE p2p_message (
    "id" INTEGER NOT NULL,
    "messageid" TEXT,
    "response_messageid" TEXT,
    "message_name" TEXT, 
    "message"  TEXT
    "queue" TEXT,
    "is_ingoing" INTEGER,
    "is_delivered" INTEGER,
    "delivery_attempts" INTEGER,
    "last_attempt_time" INTEGER
)
