<script lang="ts">
  import { onMount } from 'svelte';
  import * as ace from 'ace-builds';
  import 'ace-builds/src-noconflict/mode-sql';
  import 'ace-builds/src-noconflict/theme-chrome';
  import 'ace-builds/src-noconflict/theme-github_dark';

  interface Props {
    value: string;
    onrun: () => void;
  }

  let { value = $bindable(''), onrun }: Props = $props();

  let container: HTMLDivElement;
  let editor: ace.Ace.Editor | null = null;
  let isApplyingExternalValue = false;

  function isDarkMode() {
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  function applyTheme() {
    if (!editor) return;
    editor.setTheme(isDarkMode() ? 'ace/theme/github_dark' : 'ace/theme/chrome');
  }

  onMount(() => {
    const colorScheme = window.matchMedia('(prefers-color-scheme: dark)');

    editor = ace.edit(container, {
      mode: 'ace/mode/sql',
      theme: isDarkMode() ? 'ace/theme/github_dark' : 'ace/theme/chrome',
      value,
      fontSize: 14,
      showPrintMargin: false,
      highlightActiveLine: true,
      tabSize: 2,
      useSoftTabs: true,
      wrap: true,
    });

    editor.setOptions({
      enableBasicAutocompletion: false,
      enableLiveAutocompletion: false,
      showLineNumbers: true,
      showGutter: true,
      useWorker: false,
    });

    editor.commands.addCommand({
      name: 'runQuery',
      bindKey: { win: 'Ctrl-Enter', mac: 'Command-Enter' },
      exec: () => onrun(),
    });

    editor.session.on('change', () => {
      if (editor && !isApplyingExternalValue) {
        value = editor.getValue();
      }
    });

    const syncTheme = () => applyTheme();
    colorScheme.addEventListener('change', syncTheme);

    applyTheme();
    editor.focus();

    return () => {
      colorScheme.removeEventListener('change', syncTheme);
      editor?.destroy();
      editor = null;
    };
  });

  $effect(() => {
    if (!editor) return;
    const current = editor.getValue();
    if (current === value) return;
    isApplyingExternalValue = true;
    editor.setValue(value, -1);
    isApplyingExternalValue = false;
  });
</script>

<div class="relative h-full w-full overflow-hidden bg-base-100">
  <div bind:this={container} class="h-full w-full"></div>
</div>
