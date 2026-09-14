{{- define "gpu-telemetry.name" -}}gpu-telemetry{{- end }}
{{- define "gpu-telemetry.fullname" -}}{{ .Release.Name }}-gpu-telemetry{{- end }}
{{- define "gpu-telemetry.labels" -}}
app.kubernetes.io/name: {{ include "gpu-telemetry.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}
{{- define "gpu-telemetry.password" -}}
{{- $name := printf "%s-db" (include "gpu-telemetry.fullname" .) -}}
{{- $existing := lookup "v1" "Secret" .Release.Namespace $name -}}
{{- if and $existing $existing.data -}}
{{- index $existing.data "password" | b64dec -}}
{{- else -}}
{{- randAlphaNum 32 -}}
{{- end -}}
{{- end }}
