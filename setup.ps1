# Weather Raw Data API - Quick Setup Script
# Dit script helpt je snel te starten met Kaggle credentials

Write-Host "=== Weather Raw Data API - Setup ===" -ForegroundColor Cyan
Write-Host ""

# Check if .env exists
if (Test-Path ".env") {
    Write-Host "✓ .env bestand bestaat al" -ForegroundColor Green
} else {
    Write-Host "Maak .env bestand aan..." -ForegroundColor Yellow
    Copy-Item ".env.example" ".env"
    Write-Host "✓ .env aangemaakt van .env.example" -ForegroundColor Green
    Write-Host ""
    Write-Host "⚠️  BELANGRIJK: Open .env en vul je Kaggle credentials in!" -ForegroundColor Yellow
    Write-Host "   1. Ga naar https://www.kaggle.com/settings" -ForegroundColor Gray
    Write-Host "   2. Klik 'Create New API Token'" -ForegroundColor Gray
    Write-Host "   3. Kopieer username en key naar .env" -ForegroundColor Gray
    Write-Host ""
    
    # Optionally open .env in editor
    $openFile = Read-Host "Wil je .env nu openen in notepad? (j/n)"
    if ($openFile -eq 'j' -or $openFile -eq 'y') {
        notepad .env
    }
}

# Check if kaggle folder exists
if (-not (Test-Path "kaggle")) {
    Write-Host "Maak kaggle/ folder aan..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Path "kaggle" | Out-Null
    Write-Host "✓ kaggle/ folder aangemaakt" -ForegroundColor Green
}

# Check if data folder exists
if (-not (Test-Path "data")) {
    Write-Host "Maak data/ folder aan..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Path "data" | Out-Null
    Write-Host "✓ data/ folder aangemaakt" -ForegroundColor Green
}

Write-Host ""
Write-Host "=== Setup Compleet ===" -ForegroundColor Green
Write-Host ""
Write-Host "Volgende stappen:" -ForegroundColor Cyan
Write-Host "1. Zorg dat je Kaggle credentials in .env staan" -ForegroundColor White
Write-Host "2. Start de stack met: docker compose up --build" -ForegroundColor White
Write-Host "3. Bezoek http://localhost:8000/docs voor API documentatie" -ForegroundColor White
Write-Host ""

# Ask if user wants to start now
$start = Read-Host "Wil je de stack nu starten? (j/n)"
if ($start -eq 'j' -or $start -eq 'y') {
    Write-Host ""
    Write-Host "Starting Docker Compose..." -ForegroundColor Cyan
    docker compose up --build
}
