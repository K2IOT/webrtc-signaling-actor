{{- define "signaling.validate" -}}
{{- if not (regexMatch "^c[0-9]{3}$" .Values.cell) }}{{ fail "required cell cNNN" }}{{ end -}}
{{- if not (regexMatch "^[0-9a-f]{64}$" .Values.configurationFingerprint) }}{{ fail "required immutable configuration fingerprint" }}{{ end -}}
{{- if not (regexMatch "^sha256:[0-9a-f]{64}$" .Values.image.digest) }}{{ fail "required exact candidate image digest" }}{{ end -}}
{{- $_ := required "required candidate image repository" .Values.image.repository -}}
{{- $_ := required "required pre-provisioned native trust/runtime secret" .Values.runtime.secretName -}}
{{- $_ := required "required identity issuer" .Values.runtime.identityIssuer -}}
{{- $_ := required "required identity audience" .Values.runtime.identityAudience -}}
{{- if or (ne (int .Values.actor.replicas) 6) (ne (int .Values.actor.maxSurge) 1) (ne (int .Values.actor.minAvailable) 4) (lt (int .Values.actor.terminationGraceSeconds) 90) }}{{ fail "actor quorum/ingress contract requires 6 + 1 surge, PDB4 and >=90s grace" }}{{ end -}}
{{- if or (ne (int .Values.gateway.maxSurge) 1) (lt (int .Values.gateway.minAvailable) (int (ceil (divf (mulf (int .Values.gateway.replicas) 2) 3)))) }}{{ fail "gateway PDB must retain two thirds of configured capacity" }}{{ end -}}
{{- if not (has .Values.actor.formationProfile (list "initial-six" "join-only" "authorized-reformation")) }}{{ fail "invalid formation profile" }}{{ end -}}
{{- if or (lt (int .Values.gateway.replicas) 3) (ne (mod (int .Values.gateway.replicas) 3) 0) (lt (int .Values.gateway.terminationGraceSeconds) 300) }}{{ fail "gateway requires three-AZ capacity and 300s drain" }}{{ end -}}
{{- if or (ne (int .Values.control.replicas) 3) (ne (int .Values.control.minAvailable) 2) }}{{ fail "control requires three-AZ quorum" }}{{ end -}}
{{- $_ := required "required private Kubernetes API CIDR" .Values.network.kubernetesApiCidr -}}
{{- $_ := required "required authority CIDRs" .Values.network.authorityCidrs -}}
{{- $_ := required "required authenticated platform source CIDRs" .Values.network.platformCidrs -}}
{{- $_ := required "required private actor peer CIDRs" .Values.network.peerActorCidrs -}}
{{- $_ := required "required approved edge namespace" .Values.network.edgeNamespace -}}
{{- $_ := required "required monitoring namespace" .Values.network.monitoringNamespace -}}
{{- range concat (list .Values.network.kubernetesApiCidr) .Values.network.authorityCidrs .Values.network.platformCidrs .Values.network.peerActorCidrs }}
{{- if or (eq . "0.0.0.0/0") (eq . "::/0") (not (contains "/" .)) }}{{ fail "explicit private CIDR required; unrestricted egress forbidden" }}{{ end -}}
{{- end -}}
{{- end -}}
{{- define "signaling.labels" -}}
app: webrtc-signaling
plane: {{ .component }}
cell: {{ .root.Values.cell }}
app.kubernetes.io/name: webrtc-signaling
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
signaling.cell: {{ .root.Values.cell }}
{{- end -}}
{{- define "signaling.deployment" -}}
{{- $root := .root -}}{{- $component := .component -}}{{- $plane := index $root.Values $component -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ $root.Release.Name }}-{{ $component }}
  labels:
    {{- include "signaling.labels" . | nindent 4 }}
spec:
  replicas: {{ $plane.replicas }}
  strategy:
    type: RollingUpdate
    rollingUpdate: {maxSurge: {{ $plane.maxSurge }}, maxUnavailable: 0}
  selector:
    matchLabels:
      {{- include "signaling.labels" . | nindent 6 }}
  template:
    metadata:
      labels:
        {{- include "signaling.labels" . | nindent 8 }}
      annotations:
        signaling.config-fingerprint: {{ $root.Values.configurationFingerprint | quote }}
    spec:
      serviceAccountName: {{ $root.Release.Name }}-{{ $component }}
      automountServiceAccountToken: {{ eq $component "actor" }}
      terminationGracePeriodSeconds: {{ $plane.terminationGraceSeconds }}
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
        seccompProfile: {type: RuntimeDefault}
      topologySpreadConstraints:
        - topologyKey: topology.kubernetes.io/zone
          maxSkew: 1
          minDomains: 3
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              {{- include "signaling.labels" . | nindent 14 }}
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels:
                  {{- include "signaling.labels" . | nindent 18 }}
      containers:
        - name: {{ $component }}
          image: {{ printf "%s@%s" $root.Values.image.repository $root.Values.image.digest | quote }}
          imagePullPolicy: IfNotPresent
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities: {drop: [ALL]}
          resources:
            {{- toYaml $root.Values.resources | nindent 12 }}
          ports:
            - {name: business-tls, containerPort: 8443}
            - {name: health, containerPort: 8559}
            {{- if eq $component "actor" }}
            - {name: artery-tls, containerPort: 25520}
            - {name: management, containerPort: 8558}
            {{- end }}
          env:
            - {name: SPRING_PROFILES_ACTIVE, value: {{ $component | quote }}}
            - {name: JAVA_TOOL_OPTIONS, value: {{ $root.Values.javaOptions | quote }}}
            - {name: SIGNALING_CELL_ID, value: {{ $root.Values.cell | quote }}}
            - {name: SIGNALING_CLUSTER_FINGERPRINT, value: {{ $root.Values.configurationFingerprint | quote }}}
            - {name: SIGNALING_IDENTITY_ISSUER, value: {{ $root.Values.runtime.identityIssuer | quote }}}
            - {name: SIGNALING_IDENTITY_AUDIENCE, value: {{ $root.Values.runtime.identityAudience | quote }}}
            - {name: SIGNALING_NATIVE_FENCE_REQUIRED, value: 'true'}
            - {name: SIGNALING_MAX_INGRESS_PRODUCERS, value: '7'}
            - {name: SIGNALING_RUNTIME_CONTRACT, value: /run/signaling/runtime.yaml}
            - name: SIGNALING_POD_IP
              valueFrom: {fieldRef: {fieldPath: status.podIP}}
            - name: SIGNALING_NAMESPACE
              valueFrom: {fieldRef: {fieldPath: metadata.namespace}}
            {{- if eq $component "actor" }}
            - {name: SIGNALING_ACTOR_SERVICE, value: {{ printf "signaling-%s" $root.Values.cell | quote }}}
            - {name: SIGNALING_DISCOVERY_SELECTOR, value: {{ printf "app=webrtc-signaling,plane=actor,cell=%s" $root.Values.cell | quote }}}
            - {name: SIGNALING_FORMATION_PROFILE, value: {{ $plane.formationProfile | quote }}}
            - {name: SIGNALING_REMOTING_KEYSTORE, value: /run/signaling/remoting.p12}
            - {name: SIGNALING_REMOTING_TRUSTSTORE, value: /run/signaling/trust.p12}
            {{- range list "SIGNALING_REMOTING_KEYSTORE_PASSWORD" "SIGNALING_REMOTING_KEY_PASSWORD" "SIGNALING_REMOTING_TRUSTSTORE_PASSWORD" }}
            - name: {{ . }}
              valueFrom:
                secretKeyRef: {name: {{ $root.Values.runtime.secretName }}, key: {{ . }}}
            {{- end }}
            {{- end }}
          startupProbe:
            httpGet: {path: /live, port: health}
            periodSeconds: 5
            failureThreshold: 36
          readinessProbe:
            httpGet: {path: /ready, port: health}
            periodSeconds: 2
            failureThreshold: 1
          livenessProbe:
            httpGet: {path: /live, port: health}
            periodSeconds: 10
            failureThreshold: 3
          volumeMounts:
            - {name: native-contract, mountPath: /run/signaling, readOnly: true}
            - {name: tmp, mountPath: /tmp}
      volumes:
        - name: native-contract
          secret: {secretName: {{ $root.Values.runtime.secretName }}, defaultMode: 0440}
        - name: tmp
          emptyDir: {medium: Memory, sizeLimit: 128Mi}
{{- end -}}
