from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import weather_router
from routers.cleaned_weather_router import router as cleaned_weather_router

# Create FastAPI app
app = FastAPI(
    title="Weather Data API",
    description="API for accessing weather data - both raw text and cleaned numeric values",
    version="2.0.0"
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
app.include_router(cleaned_weather_router)  # Primary endpoint for cleaned data
app.include_router(weather_router)  # Legacy endpoint for raw data


@app.get("/", tags=["Health"])
def root():
    """Root endpoint - health check."""
    return {
        "status": "ok",
        "message": "Weather Data API is running",
        "endpoints": {
            "cleaned_data": "/weather",
            "raw_data": "/raw-weather",
            "documentation": "/docs"
        }
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
