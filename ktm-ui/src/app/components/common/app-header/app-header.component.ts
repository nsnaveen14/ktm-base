import { Component,Input,ViewChild, OnInit, OnDestroy } from '@angular/core';
import { LocalStorageService } from '../../../services/localstorage.service';
import { DataService } from '../../../services/data.service';
import { CommonModule } from '@angular/common';
import { MessageService } from '../../../services/message.service';
import { CommonReqRes } from '../../../models/CommonReqRes';
import { catchError } from 'rxjs/operators';
import { of, interval, Subscription } from 'rxjs';


@Component({
  selector: 'app-header',
  imports: [CommonModule],
  standalone: true,
  templateUrl: './app-header.component.html',
  styleUrl: './app-header.component.css'
})
export class AppHeaderComponent implements OnInit, OnDestroy {
  @Input() title: string="KTM";
  accessToken: any;
  isLoggedIn: boolean = false;
  userModel: any;
  userModelString: any;
  commonMessageText: string | null = null;
  commonMessageType: string | any; // Property to store the commonMessage text
  isTickerConnStarted: boolean = false;
  isJobRunning: boolean = false;
  
  // Add subscription for interval
  private jobParametersInterval$: Subscription = new Subscription();
  constructor(private dataService: DataService,private localStorageService: LocalStorageService, private messageService: MessageService){

    if(this.localStorageService.getItem('accessToken')==null)
    {
       this.dataService.getAccessTokenFromDB().subscribe(res=>{
        console.log('Api Response: '+res)
             if(res.userId==null)
              {
                window.location.href="/login";
              }
              else
              {
                this.localStorageService.setItem('accessToken', res.accessToken);
                this.localStorageService.setItem('apiKey', res.apiKey);
                console.log("Access Token: "+this.localStorageService.getItem('accessToken'));
                this.accessToken = this.localStorageService.getItem('accessToken');
                this.isLoggedIn = true;
                this.userModel = res;
                this.userModelString = JSON.stringify(res);
                this.localStorageService.setItem('userModel',this.userModelString );
                console.log("KiteWSURL: "+res.avatarURL);
                this.localStorageService.setItem('kiteWSURL',res.avatarURL);
              }

       });
  }
  else{
    console.log("Within else: "+this.localStorageService.getItem('accessToken'));
    this.accessToken = this.localStorageService.getItem('accessToken');
    this.isLoggedIn = true;

    this.userModelString = this.localStorageService.getItem('userModel');
    if(this.userModelString!=null)
      this.userModel = JSON.parse(this.userModelString);

    // navigate the user to home page
  }
}

ngOnInit(): void {
  // Initial call
  this.getJobRunningParameters();

  // Set up interval to call every 5 seconds
  this.jobParametersInterval$ = interval(5000).subscribe(() => {
    this.getJobRunningParameters();
  });

  this.messageService.message$.subscribe((message: CommonReqRes) => {
    this.onCommonMessageReceived(message); // Call the method when a message is received
  });
}

ngOnDestroy(): void {
  // Clean up the interval subscription to prevent memory leaks
  if (this.jobParametersInterval$) {
    this.jobParametersInterval$.unsubscribe();
  }
}

isWithinMarketHours(): boolean {
    const now = new Date();
    const istTime = new Date(now.toLocaleString("en-US", {timeZone: "Asia/Kolkata"}));
    
    const currentHour = istTime.getHours();
    const currentMinute = istTime.getMinutes();
    const currentTimeInMinutes = currentHour * 60 + currentMinute;
    
    const marketOpenTime = 9 * 60 + 15; // 9:15 AM in minutes
    const marketCloseTime = 15 * 60 + 30; // 3:30 PM in minutes
    
    return currentTimeInMinutes >= marketOpenTime && currentTimeInMinutes <= marketCloseTime;
  }

getJobRunningParameters() {

    this.dataService.getJobRunningParameters().pipe().subscribe((res: any) => {
      console.log('Job Running Parameters:', res);

      // Access properties directly
      this.isTickerConnStarted = res.hasOwnProperty('isKiteTickerRunning') ? res.isKiteTickerRunning : false;
      console.log('isTickerConnStarted:', this.isTickerConnStarted);
      
       if(this.isWithinMarketHours()) {
      this.isJobRunning = res.hasOwnProperty('isJobRunning') ? res.isJobRunning : false;

      if(!this.isJobRunning) {
          this.messageService.sendMessage(new CommonReqRes(false,"Nifty Current Week job is not running. Please start the job.",0,null,"warning")); 
       }
      
      }
    });

  }

  clearLocalStorage() {
    this.localStorageService.clear();
    window.location.reload();
  }

  login() {
    window.location.href="/login";
  }

  public onCommonMessageReceived(commonMessage: CommonReqRes) {
    console.log('Received message in AppHeaderComponent:', commonMessage);
    this.commonMessageText = commonMessage.message;
    this.commonMessageType = commonMessage.type.toLowerCase();
   
    setTimeout(() => {
      this.commonMessageText = null; // Clear the message after 5 seconds
    }, 5000);
    
  }

  startTicker() {
    this.dataService.startTickerConnection().subscribe(res=>{
      console.log("Ticker Started: "+res);
      if(res==true)
      this.isTickerConnStarted = true;
    });
  }

  stopTicker() {
    this.dataService.stopTickerConnection().subscribe(res=>{
      console.log("Ticker Stopped: "+res);
      if(res==false)
      this.isTickerConnStarted = false;
    });
  }

  takeOISnapshot(appJobConfigNum: number)
{
   this.dataService.getOISnapshot(appJobConfigNum).subscribe(res=>{

    if(res==true)
      console.log("Snapshot saved successfully");
    else
    {
      console.log("Error in saving. Please press Clear Local Storage button.");
      this.messageService.sendMessage(new CommonReqRes(false,"Error in saving. Please press Clear Local Storage button.",0,null,"error"));
    }
   });
}

triggerFileInput() {
    const fileInput = document.getElementById('uploadOIFile') as HTMLInputElement;
    if (fileInput) {
      fileInput.click();
    }
  }

  onFileSelected(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      const file = input.files[0];
      this.uploadOISnapshot(file);
    }
  }

  uploadOISnapshot(file: File) {

    this.dataService.uploadOISnapshot(file).pipe(
      catchError(error => {
        console.error('Error uploading OI Snapshot:', error);
        console.log(error);
        this.messageService.sendMessage(error.error);
        return of(null); // Return a fallback value or an empty observable
      })
    ).subscribe(res => {
      if (res) {
        this.messageService.sendMessage(res);
      }
    });
   }

startJob(appJobConfigNum: number) {
  this.dataService.startJob(appJobConfigNum).subscribe(res=>{
    console.log("Job Started: "+res);
    if(res==true)
    {
      this.isJobRunning = true;
    this.messageService.sendMessage(new CommonReqRes(false,"Job Started Successfully",0,null,"success"));
    }
    else
    this.messageService.sendMessage(new CommonReqRes(false,"Error in starting job",0,null,"error"));
  });
}

stopJob(appJobConfigNum: number) {
  this.dataService.stopJob(appJobConfigNum).subscribe(res=>{
    console.log("Job Stopped: "+res);
    if(res==true)
    {
      this.isJobRunning = false;
      this.messageService.sendMessage(new CommonReqRes(false,"Job Stopped Successfully",0,null,"success"));
    }
    else
    this.messageService.sendMessage(new CommonReqRes(false,"Error in stopping job",0,null,"error"));
  });
}

}
