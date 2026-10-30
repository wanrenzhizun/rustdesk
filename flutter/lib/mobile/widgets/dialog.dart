import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:flutter_hbb/common/widgets/api_server_warning.dart';
import 'package:flutter_hbb/common/widgets/setting_widgets.dart';
import 'package:flutter_hbb/common/widgets/toolbar.dart';
import 'package:get/get.dart';

import '../../common.dart';
import '../../models/platform_model.dart';

void _showSuccess() {
  showToast(translate("Successful"));
}

void setTemporaryPasswordLengthDialog(
    OverlayDialogManager dialogManager) async {
  List<String> lengths = ['6', '8', '10'];
  String length = await bind.mainGetOption(key: "temporary-password-length");
  var index = lengths.indexOf(length);
  if (index < 0) index = 0;
  length = lengths[index];
  dialogManager.show((setState, close, context) {
    setLength(newValue) {
      final oldValue = length;
      if (oldValue == newValue) return;
      setState(() {
        length = newValue;
      });
      bind.mainSetOption(key: "temporary-password-length", value: newValue);
      bind.mainUpdateTemporaryPassword();
      Future.delayed(Duration(milliseconds: 200), () {
        close();
        _showSuccess();
      });
    }

    return CustomAlertDialog(
      title: Text(translate("Set one-time password length")),
      content: Row(
          mainAxisAlignment: MainAxisAlignment.spaceEvenly,
          children: lengths
              .map(
                (value) => Row(
                  children: [
                    Text(value),
                    Radio(
                        value: value, groupValue: length, onChanged: setLength),
                  ],
                ),
              )
              .toList()),
    );
  }, backDismiss: true, clickMaskDismiss: true);
}

void showServerSettings(OverlayDialogManager dialogManager,
    void Function(VoidCallback) setState) async {
  Map<String, dynamic> options = {};
  try {
    options = jsonDecode(await bind.mainGetOptions());
  } catch (e) {
    print("Invalid server config: $e");
  }
  showServerSettingsWithValue(
      ServerConfig.fromOptions(options), dialogManager, setState);
}

void showServerSettingsWithValue(
    ServerConfig serverConfig,
    OverlayDialogManager dialogManager,
    void Function(VoidCallback)? upSetState) async {
  const labelWidth = 120.0;
  const labelSpacing = 8.0;
  var isInProgress = false;
  final idCtrl = TextEditingController(text: serverConfig.idServer);
  final relayCtrl = TextEditingController(text: serverConfig.relayServer);
  final apiCtrl = TextEditingController(text: serverConfig.apiServer);
  final keyCtrl = TextEditingController(text: serverConfig.key);
  final discoveryCtrl =
      TextEditingController(text: serverConfig.discoveryUrl);

  RxString idServerMsg = ''.obs;
  RxString relayServerMsg = ''.obs;
  RxString apiServerMsg = ''.obs;

  final controllers = [idCtrl, relayCtrl, apiCtrl, keyCtrl, discoveryCtrl];
  final errMsgs = [
    idServerMsg,
    relayServerMsg,
    apiServerMsg,
  ];

  dialogManager.show((setState, close, context) {
    Future<bool> submit() async {
      setState(() {
        isInProgress = true;
      });
      bool ret = await setServerConfig(
          null,
          errMsgs,
          ServerConfig(
              idServer: idCtrl.text.trim(),
              relayServer: relayCtrl.text.trim(),
              apiServer: apiCtrl.text.trim(),
              key: keyCtrl.text.trim(),
              discoveryUrl: discoveryCtrl.text.trim()));
      setState(() {
        isInProgress = false;
      });
      return ret;
    }

    Widget buildField(
        String label, TextEditingController controller, String errorMsg,
        {String? Function(String?)? validator, bool autofocus = false}) {
      if (isDesktop || isWeb) {
        return Row(
          children: [
            SizedBox(
              width: labelWidth,
              child: Text(label),
            ),
            SizedBox(width: labelSpacing),
            Expanded(
              child: serverSettingsTextFormField(
                label: label,
                controller: controller,
                errorMsg: errorMsg,
                contentPadding:
                    EdgeInsets.symmetric(horizontal: 8, vertical: 12),
                showLabelText: false,
                validator: validator,
                autofocus: autofocus,
              ).workaroundFreezeLinuxMint(),
            ),
          ],
        );
      }

      return serverSettingsTextFormField(
        label: label,
        controller: controller,
        errorMsg: errorMsg,
        validator: validator,
      ).workaroundFreezeLinuxMint();
    }

    return CustomAlertDialog(
      title: Row(
        children: [
          Expanded(child: Text(translate('ID/Relay Server'))),
          ...ServerConfigImportExportWidgets(controllers, errMsgs),
        ],
      ),
      content: ConstrainedBox(
        constraints: const BoxConstraints(minWidth: 500),
        child: Form(
          child: Obx(() => Column(
                mainAxisSize: MainAxisSize.min,
                children: [
                  buildField(translate('ID Server'), idCtrl, idServerMsg.value,
                      autofocus: true),
                  SizedBox(height: 8),
                  if (!isIOS && !isWeb) ...[
                    buildField(translate('Relay Server'), relayCtrl,
                        relayServerMsg.value),
                    SizedBox(height: 8),
                  ],
                  buildField(
                    translate('API Server'),
                    apiCtrl,
                    apiServerMsg.value,
                    validator: (v) {
                      if (v != null && v.isNotEmpty) {
                        if (!(v.startsWith('http://') ||
                            v.startsWith("https://"))) {
                          return translate("invalid_http");
                        }
                      }
                      return null;
                    },
                  ),
                  Padding(
                    padding: EdgeInsetsDirectional.only(
                      start: isDesktop || isWeb ? labelWidth + labelSpacing : 0,
                    ),
                    child: ApiServerWarning(
                      controller: apiCtrl,
                      idController: idCtrl,
                      message: translate('api_server_connection_tip'),
                      emptyMessage: translate('api_server_empty_tip'),
                    ),
                  ),
                  SizedBox(height: 8),
                  buildField('Key', keyCtrl, ''),
                  if (isInProgress)
                    Padding(
                      padding: EdgeInsets.only(top: 8),
                      child: LinearProgressIndicator(),
                    ),
                ],
              )),
        ),
      ),
      actions: [
        dialogButton('Cancel', onPressed: () {
          close();
        }, isOutline: true),
        dialogButton(
          'OK',
          onPressed: () async {
            if (await submit()) {
              close();
              showToast(translate('Successful'));
              upSetState?.call(() {});
            } else {
              showToast(translate('Failed'));
            }
          },
        ),
      ],
    );
  });
}

TextFormField serverSettingsTextFormField({
  required String label,
  required TextEditingController controller,
  required String errorMsg,
  String? Function(String?)? validator,
  bool autofocus = false,
  bool showLabelText = true,
  EdgeInsetsGeometry? contentPadding,
}) {
  return TextFormField(
    controller: controller,
    decoration: InputDecoration(
      labelText: showLabelText ? label : null,
      errorText: errorMsg.isEmpty ? null : errorMsg,
      contentPadding: contentPadding,
    ),
    validator: validator,
    autofocus: autofocus,
    keyboardType: TextInputType.visiblePassword,
    textCapitalization: TextCapitalization.none,
    autocorrect: false,
    enableSuggestions: false,
    smartDashesType: SmartDashesType.disabled,
    smartQuotesType: SmartQuotesType.disabled,
    enableIMEPersonalizedLearning: false,
    spellCheckConfiguration: const SpellCheckConfiguration.disabled(),
  );
}

void setPrivacyModeDialog(
  OverlayDialogManager dialogManager,
  List<TToggleMenu> privacyModeList,
  RxString privacyModeState,
) async {
  dialogManager.dismissAll();
  dialogManager.show((setState, close, context) {
    return CustomAlertDialog(
      title: Text(translate('Privacy mode')),
      content: Column(
          mainAxisAlignment: MainAxisAlignment.spaceEvenly,
          children: privacyModeList
              .map((value) => CheckboxListTile(
                    contentPadding: EdgeInsets.zero,
                    visualDensity: VisualDensity.compact,
                    title: value.child,
                    value: value.value,
                    onChanged: value.onChanged,
                  ))
              .toList()),
    );
  }, backDismiss: true, clickMaskDismiss: true);
}

void showDiscoveryUrlDialog(
    OverlayDialogManager dialogManager, void Function(VoidCallback) setState) async {
  final options = await bind.mainGetOptions();
  String current = '';
  try {
    final map = jsonDecode(options);
    current = (map['discovery-url'] ?? '') as String;
  } catch (e) {
    debugPrint('Invalid options: $e');
  }
  showDiscoveryUrlDialogWithValue(current, dialogManager, setState);
}

void showDiscoveryUrlDialogWithValue(
    String current, OverlayDialogManager dialogManager, void Function(VoidCallback)? upSetState) {
  const labelWidth = 120.0;
  const labelSpacing = 8.0;
  final urlCtrl = TextEditingController(text: current);
  final errMsg = ''.obs;
  dialogManager.show((setState, close, context) {
    urlCtrl.addListener(() => setState(() {}));
    Future<bool> submit() async {
      final value = urlCtrl.text.trim();
      if (value.isNotEmpty &&
          !value.startsWith('https://') &&
          !value.startsWith('http://')) {
        errMsg.value = translate('invalid_http');
        return false;
      }
      await bind.mainSetOption(key: 'discovery-url', value: value);
      return true;
    }

    Widget field() {
      final input = serverSettingsTextFormField(
        label: translate('Discovery URL'),
        controller: urlCtrl,
        errorMsg: errMsg.value,
        contentPadding: EdgeInsets.symmetric(horizontal: 8, vertical: 12),
        showLabelText: !(isDesktop || isWeb),
        autofocus: true,
      ).workaroundFreezeLinuxMint();
      if (isDesktop || isWeb) {
        return Row(
          children: [
            SizedBox(
              width: labelWidth,
              child: Text(translate('Discovery URL')),
            ),
            SizedBox(width: labelSpacing),
            Expanded(child: input),
          ],
        );
      }
      return input;
    }

    return CustomAlertDialog(
      title: Text(translate('Discovery URL')),
      content: ConstrainedBox(
        constraints: BoxConstraints(
          minWidth: 500,
          // AlertDialog otherwise stretches this one-field form to the window,
          // which leaves a large blank area under the input.
          maxHeight: isDesktop || isWeb ? 180 : double.infinity,
        ),
        child: Form(
          child: Obx(() => Column(
                mainAxisSize: MainAxisSize.min,
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  field(),
                  Padding(
                    padding: EdgeInsetsDirectional.only(
                      start: isDesktop || isWeb ? labelWidth + labelSpacing : 0,
                      top: 6,
                    ),
                    child: Text(
                      translate('discovery_url_tip'),
                      style: const TextStyle(fontSize: 12, color: Colors.grey),
                    ),
                  ),
                  if (urlCtrl.text.trim().startsWith('http://'))
                    Padding(
                      padding: EdgeInsetsDirectional.only(
                        start:
                            isDesktop || isWeb ? labelWidth + labelSpacing : 0,
                        top: 4,
                      ),
                      child: Text(
                        translate('discovery_url_insecure_tip'),
                        style:
                            const TextStyle(fontSize: 12, color: Colors.orange),
                      ),
                    ),
                ],
              )),
        ),
      ),
      actions: [
        dialogButton('Cancel', onPressed: () {
          close();
        }, isOutline: true),
        dialogButton(
          'OK',
          onPressed: () async {
            if (await submit()) {
              close();
              showToast(translate('Successful'));
              upSetState?.call(() {});
            }
          },
        ),
      ],
    );
  });
}
