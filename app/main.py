from __future__ import annotations

from pathlib import Path
import tempfile

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .config import ConfigError, Settings
from .service import FirehoseService


TEMPLATES = Jinja2Templates(directory=str((Path(__file__).parent / "templates").resolve()))


def create_app() -> FastAPI:
    app = FastAPI(title="Firehose Analysis Tool", version="0.1.0")
    app.mount(
        "/static",
        StaticFiles(directory=str((Path(__file__).parent / "static").resolve())),
        name="static",
    )

    @app.on_event("startup")
    def on_startup() -> None:
        try:
            settings = Settings.from_env()
            service = FirehoseService(settings)
            service.initialize()
        except ConfigError as exc:
            raise RuntimeError(f"Startup configuration error: {exc}") from exc

        app.state.settings = settings
        app.state.service = service

    def _service(req: Request) -> FirehoseService:
        return req.app.state.service

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request, error: str | None = None, message: str | None = None) -> HTMLResponse:
        service = _service(request)
        return TEMPLATES.TemplateResponse(
            request,
            "index.html",
            {
                "error": error,
                "message": message,
                "runs": service.list_recent_runs(limit=20),
            },
        )

    @app.post("/runs")
    async def create_run(
        request: Request,
        input_file: UploadFile = File(...),
    ):
        service = _service(request)

        filename = input_file.filename or ""
        if not filename.lower().endswith(".txt"):
            return RedirectResponse(url="/?error=Please+upload+a+.txt+NDJSON+file", status_code=303)

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".txt", delete=False) as temp_file:
            temp_path = Path(temp_file.name)
            while True:
                chunk = await input_file.read(1024 * 1024)
                if not chunk:
                    break
                temp_file.write(chunk)

        try:
            result = service.ingest_file(
                file_path=temp_path,
                source_filename=filename,
            )
        except Exception as exc:
            return RedirectResponse(url=f"/?error=Run+failed:+{str(exc)[:160]}", status_code=303)
        finally:
            temp_path.unlink(missing_ok=True)

        return RedirectResponse(url=f"/runs/{result.run_id}?message=Ingestion+completed", status_code=303)

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    def run_detail(request: Request, run_id: str, error: str | None = None, message: str | None = None) -> HTMLResponse:
        service = _service(request)
        context = service.get_run_context(run_id)
        if context is None:
            raise HTTPException(status_code=404, detail="Run not found")

        return TEMPLATES.TemplateResponse(
            request,
            "run_detail.html",
            {
                "run_context": context,
                "error": error,
                "message": message,
            },
        )

    @app.post("/runs/{run_id}/analyze")
    async def analyze_run(
        request: Request,
        run_id: str,
        app_id: str = Form(...),
        tenant_id: str | None = Form(default=None),
        event_type: str | None = Form(default=None),
        start_ts_ms: str | None = Form(default=None),
        end_ts_ms: str | None = Form(default=None),
        delay_breach_ms: str | None = Form(default=None),
    ):
        service = _service(request)

        if not app_id.strip():
            return RedirectResponse(url=f"/runs/{run_id}?error=app_id+is+required+for+analysis", status_code=303)

        try:
            filters = service.filters_from_form(
                tenant_id=tenant_id,
                event_type=event_type,
                start_ts_ms=start_ts_ms,
                end_ts_ms=end_ts_ms,
                delay_breach_ms=delay_breach_ms,
                default_filters=service.default_filters(),
            )
            service.analyze_run(run_id=run_id, app_id=app_id.strip(), filters=filters)
        except ValueError:
            return RedirectResponse(url=f"/runs/{run_id}?error=Numeric+fields+must+be+valid+integers", status_code=303)
        except Exception as exc:
            return RedirectResponse(url=f"/runs/{run_id}?error=Analysis+failed:+{str(exc)[:160]}", status_code=303)

        return RedirectResponse(url=f"/runs/{run_id}?message=Analysis+completed", status_code=303)

    @app.get("/runs/{run_id}/artifacts/{artifact_path:path}")
    def download_artifact(request: Request, run_id: str, artifact_path: str):
        service = _service(request)
        run_dir = service.output_dir_for_run(run_id).resolve()
        target = (run_dir / artifact_path).resolve()
        if not str(target).startswith(str(run_dir)):
            raise HTTPException(status_code=400, detail="Invalid artifact path")
        if not target.exists() or not target.is_file():
            raise HTTPException(status_code=404, detail="Artifact not found")
        return FileResponse(path=target)

    @app.post("/runs/{run_id}/report")
    async def generate_report(
        request: Request,
        run_id: str,
        extra_instructions: str | None = Form(default=None),
        create_pdf: str | None = Form(default=None),
    ):
        service = _service(request)
        try:
            service.generate_report(
                run_id=run_id,
                extra_instructions=extra_instructions,
                create_pdf=(create_pdf == "on"),
            )
        except Exception as exc:
            return RedirectResponse(
                url=f"/runs/{run_id}?error=Report+generation+failed:+{str(exc)[:160]}",
                status_code=303,
            )

        return RedirectResponse(url=f"/runs/{run_id}?message=Report+generated", status_code=303)

    return app


app = create_app()
