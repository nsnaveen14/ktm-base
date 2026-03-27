export class MiniDelta {
    id: number;
    deltaInstant: string;
    strikePrice: string;
    rateOI: number;
    strikePCR: number;
    callOI: number;
    putOI: number;
    callOIChange: number;
    putOIChange: number;
    callOIColor: string;
    putOIColor: string;
    callOIChangeColor: string;
    putOIChangeColor: string;
    rateOIColor: string;
    strikePCRColor: string;
    appJobConfigNum?: number; // Optional property to link with AppJobConfig


    constructor() {

        this.id = 0;
        this.deltaInstant = '';
        this.strikePrice ='';
        this.callOI =0.0;
        this.putOI = 0.0;
        this.rateOI = 0.0;
        this.strikePCR = 0.0;
        this.callOIChange = 0.0;
        this.putOIChange = 0.0;
        this.callOIColor = '';
        this.putOIColor = '';
        this.callOIChangeColor = '';
        this.putOIChangeColor = '';
        this.rateOIColor = '';
        this.strikePCRColor = '';
        this.appJobConfigNum = 0;
    }
}
