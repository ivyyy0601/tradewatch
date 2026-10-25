// 调用后端（ASP.NET）的 /api/... 接口
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export type Row = Record<string, any>;

export interface Status {
  server_time: string;
  heartbeats: Row[];
  sessions: Row[];
  prices: Row[];
  lag: Row[];
  pms: Row[];
  locates: Row[];
}

export interface Trace {
  parent: Row;
  children: Row[];
  events: Row[];
  fix: Row[];
}

@Injectable({ providedIn: 'root' })
export class ApiService {
  constructor(private http: HttpClient) {}

  status(): Observable<Status> {
    return this.http.get<Status>('/api/status');
  }

  alerts(all = false): Observable<Row[]> {
    return this.http.get<Row[]>('/api/alerts' + (all ? '?all=true' : ''));
  }

  workingParents(): Observable<Row[]> {
    return this.http.get<Row[]>('/api/parents/working');
  }

  rejectedParents(): Observable<Row[]> {
    return this.http.get<Row[]>('/api/parents/rejected');
  }

  events(): Observable<Row[]> {
    return this.http.get<Row[]>('/api/events');
  }

  trace(id: string): Observable<Trace> {
    return this.http.get<Trace>('/api/trace/' + encodeURIComponent(id));
  }

  // 值班操作：后端会发到 Kafka 的 ops.commands，由 OMS / EMS 执行
  pausePm(pm: string, reason: string, by: string): Observable<any> {
    return this.http.post(`/api/pms/${pm}/pause`, { reason, by });
  }

  resumePm(pm: string, reason: string, by: string): Observable<any> {
    return this.http.post(`/api/pms/${pm}/resume`, { reason, by });
  }

  cancelParent(id: string, reason: string, by: string): Observable<any> {
    return this.http.post(`/api/parents/${id}/cancel`, { reason, by });
  }

  resolveAlert(id: number, note: string, by: string): Observable<any> {
    return this.http.post(`/api/alerts/${id}/resolve`, { note, by });
  }
}
