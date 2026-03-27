export class AutoTradeParams {
    autoTradeFlag: boolean;
    closeTradeFlag: boolean;
    autoTradeCallSymbol: string;
    autoTradeCallLotSize: number;
    autoTradeCallEntryDelta: number;
    autoTradeCallSLDelta: number;
    autoTradePutSymbol: string;
    autoTradePutLotSize: number;
    autoTradePutEntryDelta: number;
    autoTradePutSLDelta: number;

    constructor(
        autoTradeFlag: boolean = false,
        closeTradeFlag: boolean = false,
        autoTradeCallSymbol: string = '',
        autoTradeCallLotSize: number = 0,
        autoTradeCallEntryDelta: number = 0,
        autoTradeCallSLDelta: number = 0,
        autoTradePutSymbol: string = '',
        autoTradePutLotSize: number = 0,
        autoTradePutEntryDelta: number = 0,
        autoTradePutSLDelta: number = 0
    ) {
        this.autoTradeFlag = autoTradeFlag;
        this.closeTradeFlag = closeTradeFlag;
        this.autoTradeCallSymbol = autoTradeCallSymbol;
        this.autoTradeCallLotSize = autoTradeCallLotSize;
        this.autoTradeCallEntryDelta = autoTradeCallEntryDelta;
        this.autoTradeCallSLDelta = autoTradeCallSLDelta;
        this.autoTradePutSymbol = autoTradePutSymbol;
        this.autoTradePutLotSize = autoTradePutLotSize;
        this.autoTradePutEntryDelta = autoTradePutEntryDelta;
        this.autoTradePutSLDelta = autoTradePutSLDelta;
    }
}