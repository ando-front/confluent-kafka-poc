"""05 dead letter queue — producer with intentional bad messages.

Sends order messages to ``dlq.orders.v1`` but, 20% of the time, injects a
malformed payload (invalid JSON or a schema-violating record) so the downstream
consumer has something to route to the DLQ.

Run:  python use_cases/05_dead_letter_queue/producer.py [--count 50]
"""

from __future__ import annotations

import argparse
import random
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core import BaseProducer, TopicAdmin, configure_logging  # noqa: E402

TOPIC = "dlq.orders.v1"


def main() -> None:
    parser = argparse.ArgumentParser(description="DLQ demo producer (20% bad messages)")
    parser.add_argument("--count", type=int, default=50, help="messages to send")
    parser.add_argument("--bad-rate", type=float, default=0.2, help="fraction of bad messages")
    args = parser.parse_args()

    configure_logging()
    TopicAdmin().ensure_topic_exists(TOPIC)

    good = bad = 0
    with BaseProducer(default_topic=TOPIC) as producer:
        for _ in range(args.count):
            key = str(uuid.uuid4())[:8]
            if random.random() < args.bad_rate:
                bad += 1
                kind = random.choice(["invalid_json", "missing_field"])
                if kind == "invalid_json":
                    # Send raw bytes that are not valid JSON.
                    producer.produce(value=b"bad_message:{not-json", key=key)
                else:
                    # Valid JSON but missing the required 'amount' field.
                    producer.produce(value={"order_id": key, "customer": "x"}, key=key)
                print(f"-> BAD  ({kind}) key={key}")
            else:
                good += 1
                producer.produce(
                    value={
                        "order_id": key,
                        "customer": random.choice(["alice", "bob"]),
                        "amount": round(random.uniform(1000, 9000), 2),
                    },
                    key=key,
                )
                print(f"-> good key={key}")
            time.sleep(0.05)
    print(f"Done. good={good} bad={bad}")


if __name__ == "__main__":
    main()
