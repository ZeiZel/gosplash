{{/*
НЕОЧЕВИДНОЕ РЕШЕНИЕ: fullname — это .Release.Name, БЕЗ примеси имени чарта
и без обрезки/хеширования (как делает стандартный `helm create`). В этом
проекте на namespace всегда ровно один экземпляр каждого сервиса — не
предполагается ставить wallet дважды под разными релизами в одном
неймспейсе, — а предсказуемое имя нужно ДРУГИМ чартам: order-worker
(шаги саги) обращается к wallet по headless-DNS "wallet-grpc" (см.
values.yaml order-worker: WALLET_GRPC_TARGET), и это работает только
тогда, когда релиз реально называется "wallet" (см. mk/k8s.mk: helm
upgrade --install wallet ...).
Платим за это тем, что нельзя поставить второй релиз того же чарта рядом
без конфликта имён — для этого проекта это приемлемое ограничение.
*/}}
{{- define "wallet.fullname" -}}
{{- .Release.Name -}}
{{- end -}}

{{- define "wallet.labels" -}}
app.kubernetes.io/name: wallet
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "wallet.selectorLabels" -}}
app.kubernetes.io/name: wallet
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "wallet.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "wallet.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
