from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from app.database import add_indexes, close_db
from app.scheduler import start_scheduler, stop_scheduler
from app.response import error_response

from app.routers import (
    auth,
    users,
    scripts,
    blueprints,
    steps,
    runs,
    step_runs,
    schedules,
    notifications,
    webhooks,
    websocket,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await add_indexes()
    start_scheduler()
    yield
    # Shutdown
    stop_scheduler()
    await close_db()


app = FastAPI(
    title="WorkflowOS API",
    description="Configurable workflow execution and validation management system",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(scripts.router)
app.include_router(blueprints.router)
app.include_router(steps.router)
app.include_router(runs.router)
app.include_router(step_runs.router)
app.include_router(schedules.router)
app.include_router(notifications.router)
app.include_router(webhooks.router)
app.include_router(websocket.router)


# Exception handlers

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    details = {}
    for error in exc.errors():
        field = ".".join(str(loc) for loc in error["loc"] if loc != "body")
        details[field] = error["msg"]

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content=error_response("VALIDATION_ERROR", "Request validation failed", details),
    )


@app.exception_handler(404)
async def not_found_handler(request: Request, exc):
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content=error_response("NOT_FOUND", "The requested resource was not found"),
    )


@app.exception_handler(405)
async def method_not_allowed_handler(request: Request, exc):
    return JSONResponse(
        status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
        content=error_response("METHOD_NOT_ALLOWED", "Method not allowed"),
    )


@app.exception_handler(500)
async def internal_error_handler(request: Request, exc):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_response("INTERNAL_ERROR", "An internal server error occurred"),
    )


@app.get("/health")
async def health_check():
    return {"success": True, "data": {"status": "healthy"}, "error": None, "meta": None}


@app.get("/")
async def root():
    return {
        "success": True,
        "data": {
            "name": "WorkflowOS API",
            "version": "1.0.0",
            "docs": "/docs",
        },
        "error": None,
        "meta": None,
    }
