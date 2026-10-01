# Event-Driven Patterns in Evently

A deep dive into the event-driven patterns used in this modular monolith, grounded in the actual code:

1. **Event Notification** — Orders (`Ticketing` module)
2. **Event-Carried State Transfer (ECST)** — Users → Attendance
3. **Materialized View + CQRS** — `EventStatistics` (`Attendance` module)
4. **Saga (orchestration)** — `CancelEventSaga` (`Events` module, coordinating Events ↔ Ticketing)

> Patterns 1–3 are about **how a single event moves data**. Pattern 4 is a different category: it is about **coordinating a multi-step workflow that spans modules** and keeping its state durable from start to finish.

The application (see `docs/structure.png`) is a **modular monolith**: four independently-schemad modules — `Events`, `Ticketing`, `Attendance`, `Users` — each owning its own PostgreSQL schema (`events.*`, `ticketing.*`, `attendance.*`, `users.*`), talking to each other **only through a message broker** (RabbitMQ via MassTransit). No module reaches into another module's tables.

Two levels of eventing coexist and must not be confused:

| Level | Name | Scope | Transport | Base type |
|-------|------|-------|-----------|-----------|
| Inside a module | **Domain event** | In-process, same DB transaction | MediatR (in-memory) | `IDomainEvent` |
| Between modules | **Integration event** | Cross-module, cross-schema | RabbitMQ / MassTransit | `IntegrationEvent` |

The bridge between the two levels is the **Outbox**: a domain-event handler publishes an integration event onto the bus. The bridge on the receiving side is the **Inbox**: an integration-event handler translates it back into a local command.

```
        ┌─────────────── one module ───────────────┐
Command ─► Aggregate.Raise(DomainEvent) ─► [same TX, Outbox] ─► DomainEventHandler
                                                                      │
                                                                      ▼
                                                            eventBus.PublishAsync(IntegrationEvent)
                                                                      │
        ═══════════════════ RabbitMQ (broker) ════════════════════════╪══════════
                                                                      ▼
        ┌───────────── another module ─────────────┐        Inbox ─► IntegrationEventHandler ─► Command
```

---

## Shared infrastructure: Outbox / Inbox / Idempotency

Every module carries the same reliability plumbing. Understanding it once explains all three patterns.

- **Outbox** (`.../Infrastructure/Outbox/`) — when an aggregate is saved, its raised domain events are persisted into `<schema>.outbox_messages` **inside the same transaction** as the business data. `ProcessOutboxJob` later reads them and dispatches to domain-event handlers. This guarantees the event is never lost if the process crashes after commit (the classic "dual-write" problem).
- **Inbox** (`.../Infrastructure/Inbox/`) — `IntegrationEventConsumer<T>` does **not** process an incoming broker message directly. It only writes it into `<schema>.inbox_messages` (see `IntegrationEventConsumer.cs`). `ProcessInboxJob` then hands it to the real `IntegrationEventHandler`. This decouples broker delivery from business processing and makes retries safe.
- **Idempotency** — because the broker is at-least-once, handlers can run twice.
  - `IdempotentDomainEventHandler` (decorator) records `(outbox_message_id, handler_name)` in `outbox_message_consumers`; if the row exists, it skips. This lets *N* handlers each consume the same event exactly once.
  - `IdempotentIntegrationEventHandler` does the same for the inbox side.

> **Takeaway:** all three patterns ride on top of at-least-once delivery + idempotent, transactional handlers. The pattern differences are about **what the event carries** and **who owns the resulting data**, not about the transport.

---

## Pattern 1 — Event Notification (Orders / Ticketing)

> "Something happened; if you care, come and ask me for the details."

### Definition
An **event notification** is a *thin* event. It announces that a state change occurred and carries little more than an **identifier**. Interested parties react by **calling back** to the source to fetch whatever data they need. The event is a doorbell, not a delivery.

### Trigger & workflow
Entry point: `CreateOrderCommandHandler` (`Ticketing.Application/Orders/CreateOrder/`).

1. A customer checks out. `CreateOrderCommandHandler.Handle`:
   - Opens an explicit DB transaction.
   - Loads the `Customer` and the `Cart` (cart lives in the distributed cache via `CartService`).
   - For each cart item, acquires a **pessimistic lock** on the `TicketType` (`GetWithLockAsync`) and decrements available quantity — this is where overselling is prevented.
   - Builds the `Order`, inserts it, fakes a payment (`IPaymentService.ChargeAsync`), inserts a `Payment`.
   - `SaveChangesAsync` + `CommitAsync`, then clears the cart.

2. The `Order` aggregate itself raises the notification in its factory:

```csharp
// Ticketing.Domain/Orders/Order.cs
public static Order Create(Customer customer)
{
    var order = new Order { /* ... */ Status = OrderStatus.Pending };
    order.Raise(new OrderCreatedDomainEvent(order.Id));   // ← THIN: only the Id
    return order;
}
```

`OrderCreatedDomainEvent` carries **only `OrderId`**. That is the defining trait of this pattern.

### Data flow — fan-out with call-back
Three independent domain-event handlers subscribe to the same `OrderCreatedDomainEvent`, each running once (idempotent), each **querying back** for the state it needs:

| Handler | Reaction | How it gets data |
|---------|----------|------------------|
| `CreateTicketsDomainEventHandler` | Sends `CreateTicketBatchCommand(OrderId)` to issue tickets | passes only the id |
| `SendOrderConfirmationDomainEventHandler` | Sends confirmation notification | `GetOrderQuery(OrderId)` → full `OrderResponse` |
| `OrderCreatedDomainEventHandler` | Publishes the **integration** event to the broker | `GetOrderQuery(OrderId)` → full `OrderResponse` |

```csharp
// OrderCreatedDomainEventHandler.cs — notification handler calls BACK for state
Result<OrderResponse> result = await sender.Send(new GetOrderQuery(notification.OrderId), ct);
await eventBus.PublishAsync(new OrderCreatedIntegrationEvent(/* fields copied from result.Value */), ct);
```

```mermaid
flowchart TD
    A[CreateOrderCommandHandler] -->|Order.Create| B[OrderCreatedDomainEvent<br/>ONLY OrderId]
    B --> C[CreateTicketsDomainEventHandler]
    B --> D[SendOrderConfirmationDomainEventHandler]
    B --> E[OrderCreatedDomainEventHandler]
    C -->|CreateTicketBatchCommand| C2[Tickets issued]
    D -->|GetOrderQuery| D2[(read back Order)]
    E -->|GetOrderQuery| E2[(read back Order)]
    E -->|eventBus.PublishAsync| F([OrderCreatedIntegrationEvent → broker])
```

### Why it is used here
- **Decoupling by ignorance.** `Order.Create` does not know or care that tickets, confirmations, and integration events will be produced. New reactions are added by writing a new handler — the aggregate never changes. This is open/closed at the event level.
- **Always-fresh data.** Because handlers re-read through `GetOrderQuery` at the moment they run, they never act on stale, serialized snapshots. If the order was enriched between raise and handling, they see the latest.
- **Small events.** The event payload stays tiny and stable; you don't version a big schema every time `OrderResponse` grows.

### Trade-offs
- **Chattier.** Each handler issues its own query — N handlers ⇒ N reads back to the source.
- **Temporal coupling.** The source must still be reachable/consistent when the handler runs. Here it always is, because domain-event handlers run **in-process, in the same module, right after commit**.

---

## Pattern 2 — Event-Carried State Transfer (Users → Attendance)

> "Something happened, and here is *all the data you need* — keep your own copy."

### Definition
An **ECST** event is *fat*. It carries the full business state of the change so that **downstream modules can maintain their own local replica** and never have to call back to the source. This trades storage/duplication for **autonomy**: the consumer keeps working even if the producer is down.

### Trigger & workflow (source = Users module)
Two write paths in the `Users` module raise fat domain events:

```csharp
// Users.Domain/Users/User.cs
public static User Create(string email, string firstName, string lastName, string identityId)
{
    var user = new User { /* ... */ };
    user._roles.Add(Role.Member);
    user.Raise(new UserRegisteredDomainEvent(user.Id));
    return user;
}

public void Update(string firstName, string lastName)
{
    if (FirstName == firstName && LastName == lastName) return;   // no-op guard
    FirstName = firstName;
    LastName = lastName;
    Raise(new UserProfileUpdatedDomainEvent(Id, FirstName, LastName));  // ← carries state
}
```

The domain-event handler translates the domain event into a **state-carrying integration event** and puts it on the broker:

```csharp
// Users.Application/Users/UpdateUser/UserProfileUpdatedDomainEventHandler.cs
await eventBus.PublishAsync(
    new UserProfileUpdatedIntegrationEvent(
        domainEvent.Id, domainEvent.OccurredOnUtc,
        domainEvent.UserId, domainEvent.FirstName, domainEvent.LastName),  // FULL state on the wire
    cancellationToken);
```

Compare the integration event shapes — they are deliberately **fat**:

| Integration event | Fields carried |
|-------------------|----------------|
| `UserRegisteredIntegrationEvent` | `UserId, Email, FirstName, LastName` |
| `UserProfileUpdatedIntegrationEvent` | `UserId, FirstName, LastName` |

Notice there is **no `GetUserQuery` callback** anywhere downstream — everything needed travels in the payload.

### Impact on the Attendance module (the consumer)
`Attendance` maintains its **own `Attendee` table** — a replica of the slice of user data it cares about. It builds and updates that replica purely from the incoming events:

```csharp
// Attendance.Presentation/Attendees/UserRegisteredIntegrationEventHandler.cs
await sender.Send(new CreateAttendeeCommand(
    integrationEvent.UserId, integrationEvent.Email,
    integrationEvent.FirstName, integrationEvent.LastName), ct);

// Attendance.Presentation/Attendees/UserProfileUpdatedIntegrationEventHandler.cs
await sender.Send(new UpdateAttendeeCommand(
    integrationEvent.UserId, integrationEvent.FirstName, integrationEvent.LastName), ct);
```

The command handlers just persist locally — no cross-module lookups:

```csharp
// Attendance.Application/Attendees/CreateAttendee/CreateAttendeeCommandHandler.cs
var attendee = Attendee.Create(request.AttendeeId, request.Email, request.FirstName, request.LastName);
attendeeRepository.Insert(attendee);
await unitOfWork.SaveChangesAsync(cancellationToken);
```

```mermaid
flowchart LR
    subgraph Users
      U1[User.Create / User.Update] --> U2[UserRegistered / UserProfileUpdated<br/>DomainEvent]
      U2 --> U3[DomainEventHandler]
      U3 -->|fat IntegrationEvent| BUS
    end
    BUS([RabbitMQ]) --> A1
    subgraph Attendance
      A1[Inbox → IntegrationEventHandler] --> A2[CreateAttendee / UpdateAttendee Command]
      A2 --> A3[(attendance.attendees<br/>LOCAL REPLICA)]
    end
```

The same ECST pattern also flows **Ticketing → Attendance** for tickets: `TicketCreatedDomainEventHandler` (Ticketing) publishes a fat `TicketIssuedIntegrationEvent(TicketId, CustomerId, EventId, Code)`; Attendance's `TicketIssuedIntegrationEventHandler` sends `CreateTicketCommand` and stores its own `Ticket` row. By the time anyone checks in, Attendance already holds a self-sufficient copy of attendees **and** tickets.

### Why it is used here
- **Runtime autonomy.** Attendance can validate check-ins even if the `Users` (or `Ticketing`) module is offline — it owns the data it reads.
- **No cross-schema queries.** The modular-monolith rule "a module only touches its own schema" is preserved. Attendance never selects from `users.*`.
- **Read performance at check-in.** Check-in is latency-sensitive (people queuing at a door); a local `attendees`/`tickets` table means no network hop to another module.

### Trade-offs
- **Data duplication & eventual consistency.** The attendee's name exists in both `users.users` and `attendance.attendees`. After a profile edit there is a short window where they disagree until the event is processed.
- **Payload versioning.** Fat events couple the wire schema to the producer's fields; changing them requires care (add-only, tolerant readers).
- **Ordering.** If `UserProfileUpdated` were ever processed before `UserRegistered`, the update would fail — mitigated here because registration always precedes updates and the inbox retries.

---

## Pattern 3 — Materialized View + CQRS (EventStatistics / Attendance)

> "Continuously fold a stream of events into a pre-computed, read-optimized table; serve reads straight from it."

### Definition
**CQRS** splits the model that handles writes from the model that serves reads. A **materialized view** is a denormalized, pre-aggregated table kept up to date by **projecting** events as they occur, so queries become a trivial single-row `SELECT` with no joins or aggregation at read time.

Here the read model is the `attendance.event_statistics` table, described by the plain projection class:

```csharp
// Attendance.Domain/Events/EventStatistics.cs — NOT an aggregate, no behavior/events
public sealed class EventStatistics
{
    public Guid EventId { get; private set; }
    public string Title { get; private set; }
    // ...
    public int TicketsSold { get; private set; }
    public int AttendeesCheckedIn { get; private set; }
    public List<string> DuplicateCheckInTickets { get; private set; }
    public List<string> InvalidCheckInTickets { get; private set; }
}
```

### The write side — projection handlers (`EventStatistics/Projections/`)
Each relevant domain event has a handler that mutates the view with **raw Dapper SQL** (bypassing the EF write model entirely — it is a read store):

| Domain event (trigger) | Projection handler | Effect on `event_statistics` |
|------------------------|--------------------|------------------------------|
| `EventCreatedDomainEvent` | `EventCreatedDomainEventHandler` | `INSERT` a new row, counters = 0 |
| `TicketCreatedDomainEvent` | `TicketCreatedDomainEventHandler` | recompute `tickets_sold` |
| `AttendeeCheckedInDomainEvent` | `AttendeeCheckedInDomainEventHandler` | recompute `attendees_checked_in` |
| `DuplicateCheckInAttemptedDomainEvent` | `DuplicateCheckInAttemptedDomainEventHandler` | append to `duplicate_check_in_tickets` |
| `InvalidCheckInAttemptedDomainEvent` | `InvalidCheckInAttemptedDomainEventHandler` | append to `invalid_check_in_tickets` |

```csharp
// EventCreatedDomainEventHandler.cs — seed the row
INSERT INTO attendance.event_statistics(event_id, title, ..., tickets_sold, attendees_checked_in, ...)
VALUES (@EventId, @Title, ..., 0, 0, ...)

// AttendeeCheckedInDomainEventHandler.cs — fold a check-in into the counter
UPDATE attendance.event_statistics es
SET attendees_checked_in = (
    SELECT COUNT(*) FROM attendance.tickets t
    WHERE t.event_id = es.event_id AND t.used_at_utc IS NOT NULL)
WHERE es.event_id = @EventId
```

Note these recompute the aggregate from the source `tickets` table rather than doing `+= 1`; that makes the projection **self-healing and idempotent** — replaying an event yields the same count.

### The read side — a pure query handler
Reads never touch the domain or do aggregation. `GetEventStatisticsQueryHandler` opens a raw connection and selects the single pre-built row with Dapper:

```csharp
// EventStatistics/GetEventStatistics/GetEventStatisticsQueryHandler.cs
SELECT event_id AS EventId, title AS Title, ..., tickets_sold AS TicketsSold,
       attendees_checked_in AS AttendeesCheckedIn, ...
FROM attendance.event_statistics
WHERE event_id = @EventId
```

Exposed through `EventStatistics/GetEventStatistics.cs` (Presentation) as a GET endpoint. `IQueryHandler` (read) and `ICommandHandler` (write) are separate abstractions in `Common.Application.Messaging` — that separation *is* the CQRS boundary.

```mermaid
flowchart TD
    subgraph WriteSide[Write side — event projections]
      E1[EventCreatedDomainEvent] --> P1[Insert row]
      E2[TicketCreatedDomainEvent] --> P2[recompute tickets_sold]
      E3[AttendeeCheckedInDomainEvent] --> P3[recompute attendees_checked_in]
      E4[Duplicate/Invalid CheckIn events] --> P4[append offending codes]
    end
    P1 & P2 & P3 & P4 --> MV[(attendance.event_statistics<br/>MATERIALIZED VIEW)]
    MV --> Q[GetEventStatisticsQueryHandler<br/>single-row SELECT]
    Q --> API[[GET /events/.../statistics]]
```

### Why it is used here
- **Reads are cheap and constant-time.** An organizer's live dashboard ("how many sold / checked-in / duplicates") is one indexed row lookup — no `COUNT`/`JOIN` across tickets at request time, no matter how many attendees.
- **Write and read scale independently.** Check-in throughput (writes) and dashboard polling (reads) don't contend on the same query plan.
- **Naturally event-sourced-ish audit.** The two `List<string>` columns accumulate the exact ticket codes that were duplicated or invalid — a built-in fraud/troubleshooting log produced as a side effect of projecting events.

### Trade-offs
- **Eventual consistency.** The counter lags the actual check-in by the projection delay. Fine for a stats dashboard, unacceptable for the check-in *decision itself* (which is why that decision lives in the write model — see below).
- **Duplicated/derived data** must be rebuilt if the projection logic changes (replay events / re-run the folds).
- **More moving parts.** A separate table, five projection handlers, and a query handler versus one normalized table.

---

## Pattern 4 — Saga / Process Manager (CancelEventSaga)

> "A long-running, stateful coordinator that drives a multi-step workflow across modules to a definite end — and remembers where it is even if the process restarts."

### Definition
A **saga** (here implemented as an **orchestration-based** state machine, a.k.a. a *process manager*) is a durable object that **reacts to events, keeps state, and emits commands/events** until a business process completes. Where patterns 1–3 each describe *one hop* of an event, a saga stitches *many hops across several modules* into a single coordinated transaction that cannot be done in one ACID database transaction (the steps live in different schemas / different modules).

Canceling an event is exactly such a process: it must (a) cancel the event, (b) **refund all payments**, and (c) **archive all tickets** — work that is split between the `Events` module and the `Ticketing` module. No single DB transaction can span both. The saga guarantees all steps eventually run and that the process only finishes when **both** refund and archive have completed.

This is the one place in Evently that uses **orchestration**; everything else is **choreography** (each handler independently reacts, nobody is "in charge"). The contrast is the key lesson of this section.

### The state machine
Your diagram maps 1:1 onto the MassTransit state machine in `Events.Presentation/Events/CancelEventSaga/`:

![CancelEvent saga state machine](./saga_pattern.png)

```csharp
// CancelEventSaga.cs — a MassTransitStateMachine<CancelEventState>
public State CancellationStarted { get; private set; }
public State PaymentsRefunded { get; private set; }
public State TicketsArchived { get; private set; }

public Event<EventCanceledIntegrationEvent> EventCanceled { get; private set; }
public Event<EventPaymentsRefundedIntegrationEvent> EventPaymentsRefunded { get; private set; }
public Event<EventTicketsArchivedIntegrationEvent> EventTicketsArchived { get; private set; }
public Event EventCancellationCompleted { get; private set; }   // ← composite (join) event
```

The persisted state is deliberately tiny — the saga is identified by the **EventId** and remembers only which state it is in and how far the join has progressed:

```csharp
// CancelEventState.cs
public sealed class CancelEventState : SagaStateMachineInstance, ISagaVersion
{
    public Guid CorrelationId { get; set; }          // == EventId (see CorrelateById below)
    public int Version { get; set; }                 // optimistic concurrency (ISagaVersion)
    public string CurrentState { get; set; }         // "CancellationStarted" / "PaymentsRefunded" / ...
    public int CancellationCompletedStatus { get; set; }  // bit-flags tracking the CompositeEvent
}
```

### Correlation, persistence & registration
- **Correlation** — every event in the flow is routed to the *same* saga instance by `EventId`:
  ```csharp
  Event(() => EventCanceled,          c => c.CorrelateById(m => m.Message.EventId));
  Event(() => EventPaymentsRefunded,  c => c.CorrelateById(m => m.Message.EventId));
  Event(() => EventTicketsArchived,   c => c.CorrelateById(m => m.Message.EventId));
  ```
  That is why `RefundPaymentsForEventCommandHandler`/`ArchiveTicketsForEventCommandHandler` publish their integration events with `EventId` as the correlation id — so MassTransit can find the right in-flight saga.
- **Persistence** — the saga is stored in **Redis**, not Postgres, and wired up only in the `Events` module:
  ```csharp
  // EventsModule.cs
  public static Action<IRegistrationConfigurator> ConfigureConsumers(string redisConnectionString) =>
      cfg => cfg.AddSagaStateMachine<CancelEventSaga, CancelEventState>()
                .RedisRepository(redisConnectionString);
  ```
  (Registered in `Program.cs` via `EventsModule.ConfigureConsumers(redisConnectionString)`.) Because state is external and durable, the process survives a restart mid-cancellation — the defining property of a saga versus a plain fan-out.
- **Optimistic concurrency** — `ISagaVersion.Version` guards against two events mutating the same instance concurrently (e.g. refund and archive completing at the same instant).

### Trigger & end-to-end workflow
```
Admin → CancelEvent endpoint
  │  Events: CancelEventCommandHandler → event.Cancel() → EventCanceledDomainEvent
  │  EventCanceledDomainEventHandler → publish EventCanceledIntegrationEvent
  ▼
┌─ SAGA starts (Initially / When EventCanceled) ──────────────────────────────┐
│  • publish EventCancellationStartedIntegrationEvent                          │
│  • TransitionTo(CancellationStarted)                                         │
└─────────────────────────────────────────────────────────────────────────────┘
  │
  ▼  (Ticketing reacts to EventCancellationStarted)
  Ticketing: EventCancellationStartedIntegrationEventHandler → CancelEventCommand
             → event.Cancel() → EventCanceledDomainEvent (Ticketing's own)
             ├─ RefundPaymentsEventCanceledDomainEventHandler → RefundPaymentsForEventCommand
             │     → payments.Refund(); event.PaymentsRefunded() → EventPaymentsRefundedDomainEvent
             │     → publish EventPaymentsRefundedIntegrationEvent  ───────────┐
             └─ ArchiveTicketsEventCanceledDomainEventHandler → ArchiveTicketsForEventCommand
                   → tickets.Archive(); event.TicketsArchived() → EventTicketsArchivedDomainEvent
                   → publish EventTicketsArchivedIntegrationEvent  ────────────┤
  │                                                                            │
  ▼  (both integration events flow back to the SAME saga, correlated by EventId)
┌─ SAGA collects (order-independent) ───────────────────────────────────────────┐
│  During(CancellationStarted): whichever arrives first → PaymentsRefunded       │
│                                                     or → TicketsArchived        │
│  During(PaymentsRefunded):  When(EventTicketsArchived) → TicketsArchived        │
│  During(TicketsArchived):   When(EventPaymentsRefunded) → PaymentsRefunded      │
│                                                                                 │
│  CompositeEvent(EventCancellationCompleted = PaymentsRefunded ∧ TicketsArchived)│
│  DuringAny(When EventCancellationCompleted):                                     │
│     • publish EventCancellationCompletedIntegrationEvent                         │
│     • Finalize()   → saga instance removed from Redis                            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

The crucial detail is the **fork-join / barrier**. Refund and archive run **in parallel** and may finish in **either order**. The `During(...)` blocks accept both orderings (the "X" cross-transitions in your diagram), and the `CompositeEvent` only raises `EventCancellationCompleted` once **both** flags are set:

```csharp
CompositeEvent(
    () => EventCancellationCompleted,
    state => state.CancellationCompletedStatus,   // bit-field persisted across messages
    EventPaymentsRefunded, EventTicketsArchived); // fires only when BOTH have been seen
```

```mermaid
stateDiagram-v2
    [*] --> CancellationStarted: EventCanceled / publish EventCancellationStarted
    CancellationStarted --> PaymentsRefunded: EventPaymentsRefunded
    CancellationStarted --> TicketsArchived: EventTicketsArchived
    PaymentsRefunded --> TicketsArchived: EventTicketsArchived
    TicketsArchived --> PaymentsRefunded: EventPaymentsRefunded
    PaymentsRefunded --> [*]: EventCancellationCompleted / publish EventCancellationCompleted + Finalize
    TicketsArchived --> [*]: EventCancellationCompleted / publish EventCancellationCompleted + Finalize
```

### Who publishes / consumes what

| Step | Owner module | Message | Kind |
|------|--------------|---------|------|
| Start | Events | `EventCanceledIntegrationEvent` | triggers saga |
| Saga → work | Events (saga) | `EventCancellationStartedIntegrationEvent` | command-ish event |
| Refund done | Ticketing | `EventPaymentsRefundedIntegrationEvent` | reply to saga |
| Archive done | Ticketing | `EventTicketsArchivedIntegrationEvent` | reply to saga |
| Finished | Events (saga) | `EventCancellationCompletedIntegrationEvent` | terminal broadcast |

Note `EventCancellationCompletedIntegrationEvent` currently has **no consumer** — it is a deliberate extension point (e.g. notify attendees, update a dashboard) emitted when the process reaches `Final`.

### Why a saga here (and not plain choreography)
- **Cross-module atomicity without a distributed transaction.** Refund (Ticketing payments) and archive (Ticketing tickets) plus the event cancellation (Events) cannot share one ACID transaction. The saga provides *eventual* atomicity: it won't declare "completed" until every step has.
- **A join/barrier is needed.** Pure choreography can fan work out, but "do Z only after *both* X and Y finish" needs someone holding state. The `CompositeEvent` is that barrier — impossible to express cleanly with stateless handlers.
- **Durability across time and restarts.** A cancellation may take seconds to minutes (payment gateway latency, many tickets). Redis-backed state means an app restart resumes exactly where it left off.
- **Observability / single source of truth.** `CurrentState` makes "where is this cancellation?" a single lookup, which is invaluable operationally.

### Trade-offs & notes
- **Orchestration adds a central brain.** The saga now *knows* about the Events and Ticketing contracts. That is a coupling you accept in exchange for coordination — keep it thin (it only routes events and tracks state, no business rules).
- **No compensation in this implementation.** A textbook saga also defines **compensating actions** (undo steps if a later step fails). `CancelEventSaga` models the happy-path fork-join and completion barrier; it does not roll back a refund if archiving fails. If archiving could fail permanently, you would add a failure event + compensation transition. Worth flagging as the main gap.
- **At-least-once + idempotency still apply.** The same inbox/outbox/idempotency spine protects the saga's inputs; MassTransit + `ISagaVersion` protect its state transitions.
- **Placement.** The saga lives in `Events.Presentation` (the module that "owns" the event lifecycle) even though it commands Ticketing — a reasonable ownership choice since cancellation is an Events-domain concept.

---

## The two headline scenarios end-to-end

The three patterns are not alternatives — in Evently they **compose** in a single user journey.

### Scenario A — Buying a ticket

```
CreateOrderCommandHandler (lock TicketType, charge payment, save Order)
   └─ Order raises OrderCreatedDomainEvent            ← PATTERN 1: notification (Id only)
        ├─ CreateTicketsDomainEventHandler → CreateTicketBatchCommand
        │     └─ each Ticket raises TicketCreatedDomainEvent
        │           └─ TicketCreatedDomainEventHandler → GetTicketQuery (call back)
        │                 └─ publish TicketIssuedIntegrationEvent (Id,Customer,Event,Code)  ← PATTERN 2: ECST
        │                       └─ [broker] Attendance.TicketIssuedIntegrationEventHandler
        │                             └─ CreateTicketCommand → attendance.tickets (local replica)
        │                                   └─ TicketCreatedDomainEvent (Attendance)
        │                                         └─ projection: tickets_sold++             ← PATTERN 3: materialized view
        ├─ SendOrderConfirmationDomainEventHandler → GetOrderQuery (call back)
        └─ OrderCreatedDomainEventHandler → GetOrderQuery → OrderCreatedIntegrationEvent
```

- **Inside Ticketing**, order fan-out uses **event notification**: the aggregate stays ignorant of downstream work; handlers read back through `GetOrderQuery`/`GetTicketQuery` for the freshest data. This is right *within a module* where the source is always available and reads are cheap in-process.
- **Crossing into Attendance**, tickets travel by **ECST** (`TicketIssuedIntegrationEvent` carries everything), so Attendance can build a self-sufficient copy without ever querying Ticketing.
- The arrival of that copy immediately feeds the **materialized view**, keeping `tickets_sold` current for the organizer dashboard.

### Scenario B — Checking a ticket at the door

```
CheckInAttendeeCommandHandler (Attendance)
   ├─ load Attendee + Ticket  ← both are LOCAL replicas built earlier via ECST (Patterns 2)
   └─ attendee.CheckIn(ticket)                    ← the DECISION is a strong-consistency write-model op
        ├─ wrong attendee → InvalidCheckInAttemptedDomainEvent  ─┐
        ├─ already used   → DuplicateCheckInAttemptedDomainEvent ─┤ PATTERN 1 (notification) driving
        └─ success → ticket.MarkAsUsed()                          │ PATTERN 3 (projection)
                     + AttendeeCheckedInDomainEvent  ─────────────┘
                          └─ projections update attendees_checked_in / duplicate / invalid lists
   Organizer dashboard → GetEventStatisticsQuery → single-row read from event_statistics  ← PATTERN 3 (read side)
```

Why each pattern fits *this* moment:

- **The check-in decision must be strongly consistent** — you cannot let the same ticket in twice. So it is a synchronous **write-model** command (`Attendee.CheckIn`) against Attendance's own row, using its **ECST-built local replicas**. No eventual consistency is tolerated on the *decision*.
- **The consequences of the decision are notifications** (`AttendeeCheckedIn`, `Duplicate…`, `Invalid…`) — thin domain events announcing what happened, letting any number of projections react without the check-in logic knowing about them.
- **The reporting of check-ins is a materialized view** — the counter and the duplicate/invalid code lists are folded asynchronously and served as a one-row read, so the door scanner stays fast and the organizer's live numbers stay cheap.

---

## Summary comparison

| Aspect | Event Notification (Orders) | Event-Carried State Transfer (Users→Attendance) | Materialized View + CQRS (EventStatistics) |
|--------|------------------------------|--------------------------------------------------|--------------------------------------------|
| Event payload | Thin — an **id** (`OrderCreatedDomainEvent(OrderId)`) | Fat — **full state** (`UserProfileUpdatedIntegrationEvent`) | Domain events fold into a **table**, not a payload |
| Trigger | `Order.Create` after checkout | `User.Create` / `User.Update` | `EventCreated`, `TicketCreated`, `AttendeeCheckedIn`, duplicate/invalid |
| How consumer gets data | **Calls back** (`GetOrderQuery`) | **Reads from the event**, stores a replica | Projects events into a denormalized row |
| Data ownership | Source keeps the only copy | Consumer keeps its **own copy** | Read store is a derived copy of write data |
| Coupling | Low behavioral coupling, needs source live at handling time | Runtime-autonomous, coupled to payload schema | Read/write models decoupled |
| Consistency | Fresh (reads latest) | Eventually consistent replica | Eventually consistent view |
| Best for | In-module fan-out where source is always available | Cross-module reads that must survive producer downtime | Cheap, high-volume reads / dashboards |
| Cost | Chatty (N callbacks) | Duplication + versioning | Extra tables + projection handlers |
| Where in code | `Ticketing.Application/Orders/CreateOrder/*` | `Users.IntegrationEvents/*` + `Attendance.Presentation/Attendees/*` | `Attendance.Application/EventStatistics/*` |

### Where the Saga fits

The saga is **orthogonal** to patterns 1–3 — it is not another way to move one event, but a coordinator built *on top of* them:

| Aspect | Patterns 1–3 (Notification / ECST / CQRS) | Saga (CancelEventSaga) |
|--------|-------------------------------------------|------------------------|
| Concern | Move/replicate/read data for **one** event | Coordinate a **multi-step** workflow |
| Style | **Choreography** — nobody is in charge | **Orchestration** — a central state machine |
| State | Stateless handlers | **Durable state** (Redis, `CancelEventState`) |
| Lifespan | A single message handling | Long-running, survives restarts |
| Correlation | — | By `EventId` across many messages |
| Completion | Implicit (handler returns) | Explicit (`CompositeEvent` barrier → `Finalize`) |
| Use when | Independent reactions suffice | Steps must converge / "do Z after both X and Y" |

**One-line mental model:**
- *Notification* = "call me back for details."
- *ECST* = "here's a copy, keep it."
- *Materialized view / CQRS* = "I already did the math; just read it."
- *Saga* = "I'll drive this multi-step job across modules and tell you when it's all done."

All of these sit on the same reliable spine — **Outbox → broker → Inbox**, with idempotent handlers — which is what lets a modular monolith behave like cooperating services without giving up transactional integrity inside each module. The saga adds one more ingredient on top of that spine: **durable, correlated state**, turning a scatter of independent events into a coordinated, resumable business transaction.
