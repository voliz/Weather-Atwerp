from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import weather_router

# Create FastAPI app
app = FastAPI(
    title="Weather Raw Data API",
    description="API for accessing raw weather data stored in Postgres",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(weather_router)


@app.get("/", tags=["Health"])
def root():
    """Root endpoint - health check."""
    return {
        "status": "ok",
        "message": "Weather Raw Data API is running",
        "docs": "/docs"
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
