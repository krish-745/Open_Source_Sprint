import { TaskTemplates } from './task-templates';

afterEach(() => TaskTemplates.clear());

describe('TaskTemplates', () => {
  it('registers and lists templates', () => {
    TaskTemplates.register({ name: 'email', handler: 'emailSender' });
    expect(TaskTemplates.list().map((t) => t.name)).toContain('email');
    expect(TaskTemplates.get('email')?.handler).toBe('emailSender');
  });

  it('rejects a template without name or handler', () => {
    expect(() => TaskTemplates.register({ name: '', handler: 'h' } as any)).toThrow();
    expect(() => TaskTemplates.register({ name: 'x', handler: '' } as any)).toThrow();
  });

  it('applies defaults and overlays the provided payload', () => {
    TaskTemplates.register({
      name: 'report',
      handler: 'reportGenerator',
      priority: 'high',
      defaults: { format: 'pdf', pages: 10 },
    });

    const applied = TaskTemplates.apply('report', { pages: 25 });
    expect(applied.handler).toBe('reportGenerator');
    expect(applied.priority).toBe('high');
    expect(applied.payload).toEqual({ format: 'pdf', pages: 25 });
  });

  it('validates required fields', () => {
    TaskTemplates.register({
      name: 'charge',
      handler: 'billing',
      requiredFields: ['amount'],
    });

    expect(() => TaskTemplates.apply('charge', {})).toThrow(/Missing required field: amount/);
    expect(TaskTemplates.apply('charge', { amount: 100 }).payload).toEqual({ amount: 100 });
  });

  it('throws for an unknown template', () => {
    expect(() => TaskTemplates.apply('nope')).toThrow(/not found/);
  });
});
