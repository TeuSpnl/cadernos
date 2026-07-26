# OCR via Windows.Media.Ocr (embutido no Windows 10/11).
# Uso: powershell -NoProfile -ExecutionPolicy Bypass -File ocr_windows.ps1 -Path C:\temp\pagina.png
# Requer Windows PowerShell 5.1 (NAO usar PowerShell 7).
param(
    [Parameter(Mandatory = $true)]
    [string]$Path
)

$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Path)) {
    Write-Error "Arquivo nao encontrado: $Path"
    exit 2
}

$Path = (Resolve-Path -LiteralPath $Path).Path

Add-Type -AssemblyName System.Runtime.WindowsRuntime | Out-Null

$null = [Windows.Storage.StorageFile, Windows.Storage, ContentType = WindowsRuntime]
$null = [Windows.Media.Ocr.OcrEngine, Windows.Foundation, ContentType = WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder, Windows.Foundation, ContentType = WindowsRuntime]
$null = [Windows.Graphics.Imaging.SoftwareBitmap, Windows.Foundation, ContentType = WindowsRuntime]
$null = [Windows.Storage.Streams.RandomAccessStream, Windows.Storage.Streams, ContentType = WindowsRuntime]
$null = [Windows.Globalization.Language, Windows.Foundation, ContentType = WindowsRuntime]

$getAwaiter = [WindowsRuntimeSystemExtensions].GetMember('GetAwaiter').Where({
        $PSItem.GetParameters()[0].ParameterType.Name -eq 'IAsyncOperation`1'
    }, 'First')[0]

function Await-WinRT {
    param($AsyncTask, [Type]$ResultType)
    $getAwaiter.MakeGenericMethod($ResultType).Invoke($null, @($AsyncTask)).GetResult()
}

$engine = $null
foreach ($tag in @('pt-BR', 'pt-PT', 'en-US', 'en-GB')) {
    try {
        $lang = [Windows.Globalization.Language]::new($tag)
        $engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage($lang)
        if ($engine) { break }
    } catch {}
}
if (-not $engine) {
    try { $engine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromUserProfileLanguages() } catch {}
}
if (-not $engine) {
    $disponiveis = @()
    try {
        foreach ($l in [Windows.Media.Ocr.OcrEngine]::AvailableRecognizerLanguages) {
            $disponiveis += $l.LanguageTag
        }
    } catch {}
    $lista = if ($disponiveis.Count) { $disponiveis -join ', ' } else { '(nenhum)' }
    [Console]::Error.WriteLine("OCR_ENGINE_MISSING langs=$lista")
    Write-Error "Nenhum motor OCR do Windows. Idiomas: $lista. Instale OCR pt-BR/en-US OU: pip install rapidocr-onnxruntime"
    exit 3
}

$file = Await-WinRT ([Windows.Storage.StorageFile]::GetFileFromPathAsync($Path)) ([Windows.Storage.StorageFile])
$stream = Await-WinRT ($file.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
$decoder = Await-WinRT ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
$bitmap = Await-WinRT ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
$result = Await-WinRT ($engine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])

foreach ($line in $result.Lines) {
    Write-Output $line.Text
}
