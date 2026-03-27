/// <reference types="@angular/localize" />

// Global polyfills for Node.js packages that expect a browser environment
(window as any).global = window;
(window as any).process = (window as any).process || { env: {} };
(window as any).Buffer = (window as any).Buffer || { isBuffer: () => false };

import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { AppComponent } from './app/app.component';

bootstrapApplication(AppComponent, appConfig)
  .catch((err) => console.error(err));
