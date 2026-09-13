{{/*
НЕОЧЕВИДНОЕ РЕШЕНИЕ: fullname — это .Release.Name, БЕЗ примеси имени чарта
и без обрезки/хеширования (как делает стандартный `helm create`). В этом
проекте на namespace всегда ровно один экземпляр каждого сервиса — не
предполагается ставить media дважды под разными релизами в одном
неймспейсе, — а предсказуемое имя нужно ДРУГИМ чартам: catalog обращается
к media по headless-DNS "media-grpc" (см. values.yaml catalog:
MEDIA_GRPC_TARGET), и это работает только тогда, когда релиз реально
называется "media" (см. mk/k8s.mk: helm upgrade --install media ...).
Платим за это тем, что нельзя поставить второй релиз того же чарта рядом
без конфликта имён — для этого проекта это приемлемое ограничение.
*/}}
{{- define "media.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "media.labels" -}}
app.kubernetes.io/name: media
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "media.selectorLabels" -}}
app.kubernetes.io/name: media
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "media.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "media.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
