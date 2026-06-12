import logger from '../utils/logger';

export interface TaskTemplate {
  name: string;
  handler: string;
  priority?: 'low' | 'medium' | 'high' | 'critical';
  defaults?: Record<string, any>; // default payload values
  requiredFields?: string[]; // payload keys that must be supplied
}

export interface AppliedTemplate {
  handler: string;
  payload: Record<string, any>;
  priority?: 'low' | 'medium' | 'high' | 'critical';
}

/**
 * Reusable task templates with default payload values and simple required-field
 * validation, so callers can create tasks from a named template instead of
 * repeating handler/payload boilerplate.
 */
export class TaskTemplates {
  private static templates = new Map<string, TaskTemplate>();

  static register(template: TaskTemplate): TaskTemplate {
    if (!template.name || !template.handler) {
      throw new Error('Template requires name and handler');
    }
    this.templates.set(template.name, template);
    logger.info({ template: template.name }, 'Task template registered');
    return template;
  }

  static get(name: string): TaskTemplate | undefined {
    return this.templates.get(name);
  }

  static list(): TaskTemplate[] {
    return [...this.templates.values()];
  }

  static clear(): void {
    this.templates.clear();
  }

  /**
   * Build task input from a template: merge provided payload over the template
   * defaults and validate that all required fields are present.
   */
  static apply(name: string, payload: Record<string, any> = {}): AppliedTemplate {
    const template = this.templates.get(name);
    if (!template) {
      throw new Error(`Template ${name} not found`);
    }

    const merged = { ...(template.defaults || {}), ...payload };
    for (const field of template.requiredFields || []) {
      if (merged[field] === undefined) {
        throw new Error(`Missing required field: ${field}`);
      }
    }

    return { handler: template.handler, payload: merged, priority: template.priority };
  }
}
