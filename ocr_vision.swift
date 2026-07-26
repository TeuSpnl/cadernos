import Vision
import AppKit
import Foundation

// Helper de OCR via Vision (macOS) para comprovantes cujo texto veio como curva/outline.
// Uso: ocr_vision_helper <imagem.png>
guard CommandLine.arguments.count > 1 else {
    fputs("usage: ocr_vision_helper <image>\n", stderr)
    exit(1)
}
let path = CommandLine.arguments[1]
guard let img = NSImage(contentsOfFile: path),
      let tiff = img.tiffRepresentation,
      let rep = NSBitmapImageRep(data: tiff),
      let cg = rep.cgImage else {
    fputs("fail load image\n", stderr)
    exit(2)
}
let req = VNRecognizeTextRequest()
req.recognitionLevel = .accurate
req.usesLanguageCorrection = true
req.recognitionLanguages = ["pt-BR", "en-US"]
let handler = VNImageRequestHandler(cgImage: cg, options: [:])
try handler.perform([req])
for obs in (req.results ?? []) {
    if let cand = obs.topCandidates(1).first {
        print(cand.string)
    }
}
