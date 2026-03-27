export interface OrderResponse {
    exchangeOrderId: string;
    disclosedQuantity: string;
    validity: string;
    tradingSymbol: string;
    orderVariety: string;
    orderType: string;
    triggerPrice: string;
    statusMessage: string;
    price: string;
    status: string;
    product: string;
    accountId: string;
    exchange: string;
    orderId: string;
    pendingQuantity: string;
    orderTimestamp: Date;
    exchangeTimestamp: Date;
    exchangeUpdateTimestamp: Date;
    averagePrice: string;
    transactionType: string;
    filledQuantity: string;
    quantity: string;
    parentOrderId: string;
    tag: string;
    guid: string;
    validityTTL: number;
    meta: { [key: string]: any };
    auctionNumber: string;
  
    
  }