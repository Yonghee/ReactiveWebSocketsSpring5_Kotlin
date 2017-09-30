import {Component} from '@angular/core';

@Component({
  selector: 'app-root',
  template: `
    <h1> Files </h1>
    <div *ngFor="let f of files">
      {{f.path}} <span style="font-size: smaller;">
      {{f.sessionId}}
    </span>
    </div>
  `

})
export class AppComponent {

  files: Array<FileEvent> = [];

  private ws = new WebSocket('ws://localhost:8080/ws/files');

  constructor() {
    this.ws.onmessage = (me: MessageEvent) => {
      const data = JSON.parse(me.data) as FileEvent;
      this.files.push(data);
    };
  }
}



export interface FileEvent {
  path: string;
  sessionId; string;
}
