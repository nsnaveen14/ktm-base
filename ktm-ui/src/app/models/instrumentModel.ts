
export interface InstrumentModel {
    instrument_token: number;
    exchange_token: number;
    tradingsymbol: string;
    name: string;
    last_price: number;
    tick_size: number;
    instrument_type: string;
    segment: string;
    exchange: string;
    strike: string;
    lot_size: number;
    expiry: Date;
}