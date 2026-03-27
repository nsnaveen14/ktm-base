export class CommonReqRes {
    status: boolean;
    message: string;
    qty: number;
    data: any;
    type: string;
  
    constructor(status: boolean, message: string, qty: number, data: any, type: string) {
      this.status = status;
      this.message = message;
      this.qty = qty;
      this.data = data;
      this.type = type;
    }
  }