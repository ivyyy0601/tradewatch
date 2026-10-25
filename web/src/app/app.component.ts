// 值班控制台：一个页面，每 5 秒刷新一次
// 上面：服务心跳、FIX 连接、行情、Kafka 积压；中间：PM（可以暂停 / 恢复）、告警（可以关掉）；
// 下面：执行中的母单（可以撤单）、被拒的母单、最新事件；点任何一个母单号 → 看它的完整经历
import { Component, OnDestroy, OnInit } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { forkJoin } from 'rxjs';
import { ApiService, Row, Status, Trace } from './api.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
})
export class AppComponent implements OnInit, OnDestroy {
  status: Status | null = null;
  alerts: Row[] = [];
  working: Row[] = [];
  rejected: Row[] = [];
  events: Row[] = [];
  showAllAlerts = false;

  traceId = '';
  trace: Trace | null = null;
  traceError = '';

  operator = localStorage.getItem('tw-operator') || 'ivy';    // 操作人的名字，记在这个浏览器里
  message = '';
  error = '';
  lastRefresh: Date | null = null;
  private timer: any;

  constructor(private api: ApiService) {}

  ngOnInit(): void {
    this.refresh();
    this.timer = setInterval(() => this.refresh(), 5000);
  }

  ngOnDestroy(): void {
    clearInterval(this.timer);
  }

  refresh(): void {
    forkJoin({
      status: this.api.status(),
      alerts: this.api.alerts(this.showAllAlerts),
      working: this.api.workingParents(),
      rejected: this.api.rejectedParents(),
      events: this.api.events(),
    }).subscribe({
      next: (r) => {
        this.status = r.status;
        this.alerts = r.alerts;
        this.working = r.working;
        this.rejected = r.rejected;
        this.events = r.events;
        this.lastRefresh = new Date();
        this.error = '';
      },
      error: (e) => (this.error = '刷新失败：' + (e.message || e.status)),
    });
  }

  saveOperator(): void {
    localStorage.setItem('tw-operator', this.operator);
  }

  // ---------- 显示用的小工具 ----------
  hbOk(row: Row): boolean {
    return row['age_s'] !== null && row['age_s'] <= 90;
  }

  severityClass(sev: string): string {
    return 'sev-' + (sev || '').toLowerCase();
  }

  isWarnEvent(type: string): boolean {
    return ['RISK_REJECT', 'CHILD_REJECTED', 'CANCEL_REJECTED', 'PARENT_CANCELLED'].includes(type);
  }

  // ---------- 值班操作（都要写原因，和真实工作一样：谁、什么时候、为什么） ----------
  private ask(question: string): string | null {
    const answer = window.prompt(question);
    return answer && answer.trim() ? answer.trim() : null;
  }

  private done(text: string): void {
    this.message = text;
    this.error = '';
    setTimeout(() => this.refresh(), 1500);
  }

  private failed(e: any): void {
    this.error = '操作失败：' + (e.error?.error || e.message || e.status);
  }

  pause(pm: string): void {
    const reason = this.ask(`暂停 ${pm}（kill switch）。原因？（已经和 PM / manager 确认了吗？）`);
    if (!reason) return;
    this.api.pausePm(pm, reason, this.operator).subscribe({
      next: () => this.done(`已发送：暂停 ${pm}`),
      error: (e) => this.failed(e),
    });
  }

  resume(pm: string): void {
    const reason = this.ask(`恢复 ${pm}。原因？（问题已经查清楚并处理了吗？）`);
    if (!reason) return;
    this.api.resumePm(pm, reason, this.operator).subscribe({
      next: () => this.done(`已发送：恢复 ${pm}`),
      error: (e) => this.failed(e),
    });
  }

  cancel(parentId: string): void {
    const reason = this.ask(`撤掉母单 ${parentId} 和它的子单。原因？`);
    if (!reason) return;
    this.api.cancelParent(parentId, reason, this.operator).subscribe({
      next: () => this.done(`已发送：撤单 ${parentId}`),
      error: (e) => this.failed(e),
    });
  }

  resolve(alert: Row): void {
    const note = this.ask(`关掉告警 #${alert['alert_id']}（${alert['rule']}）。你查到了什么、做了什么？`);
    if (!note) return;
    this.api.resolveAlert(alert['alert_id'], note, this.operator).subscribe({
      next: () => this.done(`告警 #${alert['alert_id']} 已关闭`),
      error: (e) => this.failed(e),
    });
  }

  toggleAllAlerts(): void {
    this.showAllAlerts = !this.showAllAlerts;
    this.refresh();
  }

  // ---------- 追踪一张单 ----------
  openTrace(id: string): void {
    if (!id) return;
    this.traceId = id;
    this.traceError = '';
    this.api.trace(id.trim()).subscribe({
      next: (t) => (this.trace = t),
      error: (e) => {
        this.trace = null;
        this.traceError = e.error?.error || '查询失败';
      },
    });
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  closeTrace(): void {
    this.trace = null;
    this.traceError = '';
  }
}
