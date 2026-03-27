export interface OrderRequest{
    tradingSymbol: string;
    exchange: string;
    transaction_type: string;
    order_type: string;
    quantity: number;
    product: string;
    price: number;
    trigger_price: number;
    validity: string;
  }