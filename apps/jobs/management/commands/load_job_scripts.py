import ast
from pathlib import Path

from django.core.management.base import BaseCommand

from apps.jobs.models import JobScript


PREFERRED_CALLABLE_NAMES = [
    'run', 'main', 'start', 'execute', 'scrape', 'crawl', 'job', 'process'
]


def find_best_callable(file_path: Path) -> str:
    """Return the best callable name defined at module top-level.

    Preference order is in PREFERRED_CALLABLE_NAMES. Only callables with
    zero required positional args are considered.
    Fallback to 'run' if none found.
    """
    try:
        source = file_path.read_text(encoding='utf-8', errors='ignore')
        tree = ast.parse(source)
    except Exception:
        return 'run'

    candidates = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            # Count required positional-only and positional-or-keyword args
            num_required = len([
                a for a in node.args.args[: len(node.args.args) - len(node.args.defaults)]
            ])
            if num_required == 0:
                candidates[node.name] = True

    for name in PREFERRED_CALLABLE_NAMES:
        if name in candidates:
            return name

    # If any zero-arg function exists, pick the first deterministically
    if candidates:
        return sorted(candidates.keys())[0]

    return 'run'


class Command(BaseCommand):
    help = "Scan the script/ folder and upsert JobScript entries for each .py file"

    def add_arguments(self, parser):
        parser.add_argument(
            '--script-dir',
            default=str(Path(__file__).resolve().parents[4] / 'script'),
            help='Path to the scripts directory to scan',
        )
        parser.add_argument(
            '--dry-run', action='store_true', help='Show what would be inserted without writing'
        )
        parser.add_argument(
            '--force-run',
            action='store_true',
            help="Force module_path to use ':run' for all discovered scripts"
        )
        parser.add_argument(
            '--rewrite-existing',
            action='store_true',
            help="Rewrite existing JobScript.module_path entries ending with ':main' to ':run'"
        )

    def handle(self, *args, **options):
        script_dir = Path(options['script_dir']).resolve()
        if not script_dir.exists():
            self.stderr.write(self.style.ERROR(f"Script directory not found: {script_dir}"))
            return

        # Optional one-time rewrite of existing records from :main -> :run
        if options.get('rewrite_existing'):
            if options['dry_run']:
                count = JobScript.objects.filter(module_path__endswith=':main').count()
                self.stdout.write(
                    f"Would rewrite {count} existing module_path values from ':main' to ':run'"
                )
            else:
                qs = JobScript.objects.filter(module_path__endswith=':main')
                rewritten = 0
                for js in qs:
                    base = js.module_path.rsplit(':', 1)[0]
                    js.module_path = f"{base}:run"
                    js.save(update_fields=['module_path', 'updated_at'])
                    rewritten += 1
                self.stdout.write(self.style.SUCCESS(
                    f"Rewrote {rewritten} existing JobScript entries to use ':run'"
                ))

        created, updated = 0, 0
        for file_path in script_dir.glob('*.py'):
            if file_path.name.startswith('__'):
                continue
            name = file_path.stem.replace('_', ' ').title()
            if options.get('force_run'):
                callable_name = 'run'
            else:
                callable_name = find_best_callable(file_path)
            module_path = f"script.{file_path.stem}:{callable_name}"

            if options['dry_run']:
                self.stdout.write(f"Would upsert: name='{name}', module_path='{module_path}'")
                continue

            # Use name as the natural key to avoid conflicts when module_path changes
            obj, was_created = JobScript.objects.update_or_create(
                name=name,
                defaults={
                    'module_path': module_path,
                    'description': f"Auto-registered from {file_path.name}",
                    'is_active': True,
                },
            )
            if was_created:
                created += 1
            else:
                updated += 1

        if options['dry_run']:
            self.stdout.write(self.style.SUCCESS(
                f"Scanned {script_dir}. No changes written due to --dry-run"
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f"Processed scripts in {script_dir}. Created: {created}, Updated: {updated}."
            ))


