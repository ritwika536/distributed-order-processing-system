# Distributed Order Processing System

I built this to actually understand how backend systems handle the messy parts of processing an order — not just "take payment, done," but what happens when payment fails halfway through, or two people try to buy the last item at the same time, or a service just dies mid-request. It's a simplified e-commerce order flow split into three services that talk to each other over a message queue instead of one big app doing everything.

## How it's put together
Client → Order Service → reserves stock → Inventory Service (PostgreSQL)
↓
sends a "charge this order" message to RabbitMQ
↓
Payment Service picks it up whenever it's free
↓
sends the result back
↓
Order Service updates the order's status (stored in Redis)



- **Order Service** — the one the customer actually talks to. Reserves inventory, kicks off payment, keeps track of order status
- **Inventory Service** — owns the stock numbers, backed by PostgreSQL
- **Payment Service** — fakes charging a card (80% success rate, just to simulate real failures), listens for work on the queue instead of waiting for direct calls
- **RabbitMQ** — sits between Order and Payment so they're not directly dependent on each other being up at the same moment
- **Redis** — used for idempotency (remembering which orders I've already processed) and for storing order status

## Things I ran into and had to actually solve

**Orders don't wait around for payment anymore.** I started with Order Service blocking until payment finished, which felt fine until I load tested it and saw multi-second response times. Now it accepts the order instantly (`PENDING`), and payment happens in the background — you check `GET /orders/{id}` a moment later to see if it actually went through.

**Duplicate requests shouldn't double-charge anyone.** If the same `order_id` comes in twice (network retry, whatever), I check Redis first and just return whatever happened the first time instead of reprocessing.

**Two people can't both buy the last item.** This one took a bit to get right — my first version read the stock, checked it, then wrote the update as two separate steps, which is exactly the kind of thing that breaks under real concurrency. Fixed it by pushing the check into the same atomic database update. I wrote a test that fires 10 requests at once for a single unit of stock — only one is ever allowed to succeed.

**If payment fails after inventory's already been reserved, it gets released back** — with retries in case the release call itself fails, and if it fails 3 times in a row, it gets logged somewhere I can actually go check later instead of just silently losing that stock forever.

**I killed Payment Service on purpose to see what would happen.** Placed an order while it was down (still worked, stayed PENDING), then brought it back up — it picked the message right back up from the queue and finished the order with no data lost. RabbitMQ just holds onto messages until something's there to process them.

## Performance — found 3 real bottlenecks by actually load testing

I ran Locust against it with 50 concurrent users and the first result was rough — median response time was around 5 seconds. Instead of just accepting that, I dug into why:

1. **I was opening a brand new RabbitMQ connection on every single request.** Fixed by reusing connections.
2. **That fix introduced a new bug** — pika connections aren't thread-safe, so sharing one across threads was silently corrupting things and causing failures. Fixed by giving each thread its own connection.
3. **The real fix was architectural** — even with connections reused, blocking the whole request on payment was the core problem. Redesigning it to accept-then-process-async is what actually got response times down to ~23ms median.

I also found that under sustained heavy load (4,000+ requests over a minute), the 95th percentile climbs back up to a couple seconds — traced that to FastAPI's thread pool getting saturated. I know the real fix is fully async I/O throughout, which is a bigger rewrite than made sense to do here, so I'm noting it honestly rather than pretending it's not there.

## Testing

I wrote automated tests (pytest) instead of just manually clicking through Swagger UI every time — covers the full order flow, idempotency, insufficient stock handling, and the concurrency race condition.

```bash
cd tests
pip install -r requirements.txt
pytest test_order_system.py -v
```

## Stack
Python, FastAPI, PostgreSQL, Redis, RabbitMQ, SQLAlchemy, Docker, pytest, Locust

## What I'd still add if I kept going
- Fully async I/O to fix that thread-pool bottleneck properly
- Running multiple instances of each service (never actually tested horizontal scaling)
- A circuit breaker for when a dependency is slow rather than fully down
- Basic auth — right now anything can call any endpoint
- Some kind of alert if an order sits in PENDING too long

## Running it yourself
Needs Docker and Python 3.10+.

Spin up the dependencies:
```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=devpassword -e POSTGRES_DB=inventory -p 5432:5432 postgres
docker run -d --name redis -p 6379:6379 redis
docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

For each service folder (`inventory_service`, `payment_service`, `order_service`):
```bash
cd <service_folder>
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
uvicorn main:app --reload --port <8001|8002|8000>
```

Seed a product:
```bash
POST http://localhost:8001/seed
{"product_id": "sku-1", "name": "Mouse", "stock": 50}
```

Place an order:
```bash
POST http://localhost:8000/orders
{"order_id": "order-1", "product_id": "sku-1", "quantity": 2, "amount": 20.00}
```