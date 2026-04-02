import DOMPurify from 'dompurify';
import { marked } from 'marked';

export function renderMarkdown(markdown: string): string {
  if (!markdown.trim()) {
    return '';
  }

  const html = marked.parse(markdown, { async: false });
  return DOMPurify.sanitize(typeof html === 'string' ? html : String(html));
}