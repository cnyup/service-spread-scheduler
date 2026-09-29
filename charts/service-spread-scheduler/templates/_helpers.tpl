{{/* kubeconfig block shared by the scheduler ConfigMap. */}}
{{- define "service-spread-scheduler.kubeconfig" -}}
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: true
  resourceLock: leases
  resourceNamespace: {{ .Values.global.namespace }}
  resourceName: {{ .Values.global.schedulerName }}
profiles:
  - schedulerName: {{ .Values.global.schedulerName }}
    plugins:
      multiPoint:
        enabled:
          - name: ServiceSpread
      postFilter:
        disabled:
          - name: DefaultPreemption
    pluginConfig:
      - name: ServiceSpread
        args:
          apiVersion: config.scheduling.soyup.top/v1alpha1
          kind: ServiceSpreadArgs
          serviceLabelKey: {{ .Values.global.serviceLabelKey }}
          requirePolicy: {{ .Values.scheduler.args.requirePolicy }}
          managedSchedulerName: {{ .Values.global.schedulerName }}
          fallbackCacheTTL: {{ .Values.scheduler.args.fallbackCacheTTL }}
          reservationTTL: {{ .Values.scheduler.args.reservationTTL }}
          reconcilePeriod: {{ .Values.scheduler.args.reconcilePeriod }}
          exportNodePods: {{ .Values.scheduler.args.exportNodePods }}
          exportTargetDetail: {{ .Values.scheduler.args.exportTargetDetail }}
{{- end -}}

{{- define "service-spread-scheduler.schedulerImage" -}}
{{ .Values.scheduler.image.registry }}/{{ .Values.scheduler.image.repository }}:{{ .Values.scheduler.image.tag | default .Chart.AppVersion }}
{{- end -}}

{{- define "service-spread-scheduler.webhookImage" -}}
{{ .Values.webhook.image.registry }}/{{ .Values.webhook.image.repository }}:{{ .Values.webhook.image.tag | default .Chart.AppVersion }}
{{- end -}}
