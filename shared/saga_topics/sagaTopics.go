package saga_topics

const (
	TopicOrderEvents  = "order.events"
	TopicSagaCommands = "order.saga.commands"
	TopicSagaReplies  = "order.saga.replies"
	TopicOrderStatus  = "order.status"
)

const (
	EventOrderCreated       = "OrderCreated"
	CommandReserveStock     = "ReserveStock"
	EventStockReserved      = "StockReserved"
	EventStockRejected      = "StockRejected"
	EventOrderStatusChanged = "OrderStatusChanged"
)
